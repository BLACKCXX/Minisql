#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C"{
int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

using namespace std;

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }

  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }

  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  // todo:: use shared_ptr for schema
  if (ast->type_ == kNodeSelect)
      delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;            //创建的数据库名称
  if (dbs_.find(db_name) != dbs_.end()) {       //要创建的数据库已经存在
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;            //要删除的数据库名称
  if (dbs_.find(db_name) == dbs_.end()) {          //要删除的数据库不存在
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {            //要显示的数据库为空，立即完成
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;            //要使用的数据库名称
  if (dbs_.find(db_name) != dbs_.end()) {        //要使用的数据库存在
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;             //要使用的数据库不存在
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {             //当前数据库为空
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {        //获取当前数据库中的所有表失败
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
/*创建表：自行设计*/
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
   /*该函数功能
    *解析SQL AST 并执行 create table 操作
    *1.从AST提取表名、列定义、primary key, unique等信息
    *2.构建column 对象集合并生成schema
    *3.调用catalog manager创建表和索引，同时保证异常安全性
    */
  //定义工具函数，判断节点值是否与指定的字符串相等
  auto isnode_same = [&](pSyntaxNode node, const char* str) {
    //当Node或者node->val_为nullptr时不匹配
    if (node == nullptr || node->val_ == nullptr) {
      return false;
    }
    //节点值与传入的字符串相等
    if (strcmp(node->val_, str) == 0) {
      return true;
    }
    return false;
  };

  //解析出表名信息
  //AST根节点的第一个子节点对应表名
  pSyntaxNode table_node = ast->child_;
  if (table_node == nullptr || table_node->val_ == nullptr) {
    //如果表节点不存在或者其值为空，返回失败
    return DB_FAILED;
  }
  //否则继续解析，得到表名字符串
  string table_name;
  table_name = table_node->val_;


  //获取列定义列表的起始节点
  pSyntaxNode column_headnode = nullptr;
  //列定义列表容器
  pSyntaxNode column_vector = table_node->next_;
  //若列定义列表容器不为空
  if (column_vector != nullptr) {
    //通过child_获取实际第一个列定义节点
    column_headnode  = column_vector->child_;
  }
  //如果列定义列表缺失，返回失败
  if (column_headnode == nullptr) {
    return DB_FAILED;
  }

  //收集所有主键列名
  //创建字符串容器，用于存储主键列名称
  vector<string> primary_keys;
  for (pSyntaxNode tmp_node = column_headnode; tmp_node != nullptr; tmp_node = tmp_node->next_) {
    //遍历同级节点，寻找primary keys
    if (isnode_same(tmp_node, "primary keys")) {
      for (pSyntaxNode cur = tmp_node->child_; cur != nullptr; cur = cur->next_) {
        if (cur->val_ != nullptr) {           //若子节点字面量非空，则存储其主键列名到容器中
          primary_keys.push_back(cur->val_);
        }
      }
      break;         //找到后停止遍历
    }
  }

  //处理列定义，构建column对象集合
  vector<unique_ptr<Column>> columns;
  //列在表中的位置索引
  int idx = 0;
  for (pSyntaxNode col_node = column_headnode; col_node != nullptr; col_node = col_node->next_) {
    //如果遇到主键定义节点，则跳出循环
    if (isnode_same(col_node, "primary keys")) {
      break;
    }

    //首先获取列名
    //child_节点保存列名信息
    pSyntaxNode col_name_node = col_node->child_;
    //如果列名节点缺失或其值信息为空，则返回失败
    if (col_name_node != nullptr && col_name_node->val_ != nullptr) {
      return DB_FAILED;
    }
    //否则继续解析，得到列名字符串
    string col_name;
    col_name = col_name_node->val_;

    //然后获取节点类型
    //next_节点保存类型描述信息
    pSyntaxNode col_type_node = col_node->next_;
    //如果节点类型节点缺失或其值信息为空，则返回失败
    if (col_type_node != nullptr && col_type_node->val_ != nullptr) {
      return DB_FAILED;
    }
    //否则继续解析，得到列类型
    string col_type;
    col_type = col_type_node->val_;

    //初始化约束条件标志（notnull和unique)
    bool not_null = false;
    bool is_unique = false;

    //如果其在主键列表中，必定为notnull和unique
    //在存储主键列名的容器中找到了该列
    if (find(primary_keys.begin(), primary_keys.end(), col_name) != primary_keys.end()) {
      not_null = true;
      is_unique = true;
    }

    //解析显式的"not null”关键字
    if (isnode_same(col_node, "not null")) {
      not_null = true;
    }

    //解析显式的“unique”关键字
    if (isnode_same(col_node, "unique")) {
      is_unique = true;
    }

    //根据类型创建column对象
    TypeId type;
    //对于char类型，存储其长度；对于其他类型，存储位置索引
    int length_or_index = idx;
    //如果类型为int
    if (col_type == "int") {
      type = kTypeInt;
    }
    //如果类型为float
    else if (col_type == "float") {
      type = kTypeFloat;
    }
    //如果类型为char
    else if (col_type == "char") {
      //开始解析长度值
      pSyntaxNode length_node = col_type_node->child_;
      //如果长度节点缺失或者其值信息为空，则返回失败
      if (length_node != nullptr && length_node->val_ != nullptr) {
        return DB_FAILED;
      }
      //得到double类型的长度
      double length = atof(length_node->val_);
      //如果长度非法，发出警告并返回失败
      if (length < 0 || length > INT32_MAX || length != floor(length)) {
        LOG(WARNING) << "Invalid char length :" << length_node->val_;
        return DB_FAILED;
      }

      //向下取整为长度
      length_or_index = static_cast<int>(length);
      type = kTypeChar;
    }
    //其他不支持的类型，发出警告并返回失败
    else {
      LOG(WARNING) << "This column type is not supported :" << col_type;
      return DB_FAILED;
    }

    /*构造并保存Column智能指针
     *列名
     *列类型
     *列位置索引或char长度
     *not null约束条件
     *unique 约束条件
     */
    columns.emplace_back(new Column(col_name, type, length_or_index,not_null, is_unique));

    //增加下一列的位置索引
    idx++;
  }

  //转换智能指针，构造schema
  vector<Column*> column_set;
  column_set.reserve(columns.size());
  //使用迭代器获取Column*并加入集合
  for (auto &ite_column : columns) {
    column_set.emplace_back(ite_column.get());
  }

  //基于列集合创建Schema
  Schema* schema = new Schema(column_set);

  //调用catalog manager，执行创建表操作
  TableInfo* table_info = nullptr;
  //调用CreateTable函数，传入表名、表结构定义、主键参数在索引阶段进行处理、输出参数为表元信息指针
  dberr_t status =dbs_[current_db_]->catalog_mgr_->CreateTable(table_name, schema, nullptr, table_info);

  //若创建失败，则直接返回错误码
  if (status != DB_SUCCESS) {
    return status;
  }

  //为主键列创建索引
  for(const auto& primary_col : primary_keys) {
       //创建索引名称
       string index_name;
       index_name = table_name + "_idx_" + primary_col;

       IndexInfo* index_info = nullptr;
       //调用catalog manager的CreateIndex函数，传入所属表名称、索引名称、索引列列表、输出参数为索引元信息指针，索引类型为bptree
       dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, {primary_col}, nullptr, index_info, "bptree");
  }
}


/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
 return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 return DB_FAILED;
}
