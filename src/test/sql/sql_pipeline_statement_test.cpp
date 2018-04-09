#include <memory>
#include <string>
#include <utility>
#include "../base_test.hpp"

#include "SQLParser.h"
#include "SQLParserResult.h"
#include "gtest/gtest.h"

#include "logical_query_plan/join_node.hpp"
#include "operators/abstract_join_operator.hpp"
#include "operators/print.hpp"
#include "operators/validate.hpp"
#include "scheduler/current_scheduler.hpp"
#include "scheduler/job_task.hpp"
#include "scheduler/node_queue_scheduler.hpp"
#include "scheduler/topology.hpp"
#include "sql/sql.hpp"
#include "sql/sql_pipeline_statement.hpp"
#include "storage/storage_manager.hpp"

namespace {
// This function is a slightly hacky way to check whether an LQP was optimized. This relies on JoinDetectionRule and
// could break if something is changed within the optimizer.
// It assumes that for the query: SELECT * from a, b WHERE a.a = b.a will be translated to a Cross Join with a filter
// predicate and then optimized to a Join.
std::function<bool(const std::shared_ptr<opossum::AbstractLQPNode>&)> contains_cross =
    [](const std::shared_ptr<opossum::AbstractLQPNode>& node) {
      if (node->type() != opossum::LQPNodeType::Join) return false;
      if (auto join_node = std::dynamic_pointer_cast<opossum::JoinNode>(node)) {
        return join_node->join_mode() == opossum::JoinMode::Cross;
      }
      return false;
    };
}  // namespace

namespace opossum {

class SQLPipelineStatementTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_a = load_table("src/test/tables/int_float.tbl", 2);
    StorageManager::get().add_table("table_a", _table_a);

    _table_b = load_table("src/test/tables/int_float2.tbl", 2);
    StorageManager::get().add_table("table_b", _table_b);

    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("a", DataType::Int);
    column_definitions.emplace_back("b", DataType::Float);
    column_definitions.emplace_back("bb", DataType::Float);
    _join_result = std::make_shared<Table>(column_definitions, TableType::Data);

    _join_result->append({12345, 458.7f, 456.7f});
    _join_result->append({12345, 458.7f, 457.7f});

    _int_float_column_definitions.emplace_back("a", DataType::Int);
    _int_float_column_definitions.emplace_back("b", DataType::Float);

    _select_parse_result = std::make_shared<hsql::SQLParserResult>();
    hsql::SQLParser::parse(_select_query_a, _select_parse_result.get());

    _multi_statement_parse_result = std::make_shared<hsql::SQLParserResult>();
    hsql::SQLParser::parse(_multi_statement_dependant, _multi_statement_parse_result.get());

    SQLQueryCache<SQLQueryPlan>::get().clear();
  }

  std::shared_ptr<Table> _table_a;
  std::shared_ptr<Table> _table_b;
  std::shared_ptr<Table> _join_result;

  TableColumnDefinitions _int_float_column_definitions;

  const std::string _select_query_a = "SELECT * FROM table_a";
  const std::string _invalid_sql = "SELECT FROM table_a";
  const std::string _join_query =
      "SELECT table_a.a, table_a.b, table_b.b AS bb FROM table_a, table_b WHERE table_a.a = table_b.a AND table_a.a "
      "> 1000";
  const std::string _multi_statement_query = "INSERT INTO table_a VALUES (11, 11.11); SELECT * FROM table_a";
  const std::string _multi_statement_dependant = "CREATE VIEW foo AS SELECT * FROM table_a; SELECT * FROM foo;";

  const std::vector<std::string> _join_column_names{"a", "b", "bb"};

  std::shared_ptr<hsql::SQLParserResult> _select_parse_result;
  std::shared_ptr<hsql::SQLParserResult> _multi_statement_parse_result;

  static bool _contains_validate(const std::vector<std::shared_ptr<OperatorTask>>& tasks) {
    for (const auto& task : tasks) {
      if (std::dynamic_pointer_cast<Validate>(task->get_operator())) return true;
    }
    return false;
  }
};

TEST_F(SQLPipelineStatementTest, SimpleCreation) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_sql_string(), _select_query_a);
}

TEST_F(SQLPipelineStatementTest, SimpleCreationWithoutMVCC) {
  auto sql_pipeline = SQL{_join_query}.disable_mvcc().pipeline_statement();

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_sql_string(), _join_query);
}

TEST_F(SQLPipelineStatementTest, SimpleCreationWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  auto sql_pipeline = SQL{_select_query_a}.set_transaction_context(context).pipeline_statement();

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.get_sql_string(), _select_query_a);
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreation) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement(_select_parse_result);

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_parsed_sql_statement().get(), _select_parse_result.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreationWithoutMVCC) {
  auto sql_pipeline = SQL{_select_query_a}.disable_mvcc().pipeline_statement(_select_parse_result);

  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline.get_parsed_sql_statement().get(), _select_parse_result.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreationWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  auto sql_pipeline = SQL{_select_query_a}.set_transaction_context(context).pipeline_statement(_select_parse_result);

  EXPECT_EQ(sql_pipeline.transaction_context().get(), context.get());
  EXPECT_EQ(sql_pipeline.get_parsed_sql_statement().get(), _select_parse_result.get());
}

TEST_F(SQLPipelineStatementTest, SimpleParsedCreationTooManyStatements) {
  EXPECT_THROW(SQL(_multi_statement_dependant).disable_mvcc().pipeline_statement(_multi_statement_parse_result),
               std::exception);
}

TEST_F(SQLPipelineStatementTest, ConstructorCombinations) {
  // Simple sanity test for all other constructor options

  const auto optimizer = Optimizer::create_default_optimizer();
  auto prepared_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);
  auto transaction_context = TransactionManager::get().new_transaction_context();

  // No transaction context
  auto sql_pipeline1 = SQL{_select_query_a}.set_optimizer(optimizer).pipeline_statement();
  EXPECT_EQ(sql_pipeline1.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline1.get_sql_string(), _select_query_a);

  auto sql_pipeline2 =
      SQL{_select_query_a}.set_prepared_statement_cache(prepared_cache).disable_mvcc().pipeline_statement();
  EXPECT_EQ(sql_pipeline2.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline2.get_sql_string(), _select_query_a);

  auto sql_pipeline3 =
      SQL{_select_query_a}.set_optimizer(optimizer).set_prepared_statement_cache(prepared_cache).pipeline_statement();
  EXPECT_EQ(sql_pipeline3.transaction_context(), nullptr);
  EXPECT_EQ(sql_pipeline3.get_sql_string(), _select_query_a);

  // With transaction context
  auto sql_pipeline4 =
      SQL{_select_query_a}.set_optimizer(optimizer).set_transaction_context(transaction_context).pipeline_statement();
  EXPECT_EQ(sql_pipeline4.transaction_context(), transaction_context);
  EXPECT_EQ(sql_pipeline4.get_sql_string(), _select_query_a);

  auto sql_pipeline5 = SQL{_select_query_a}
                           .set_prepared_statement_cache(prepared_cache)
                           .set_transaction_context(transaction_context)
                           .pipeline_statement();
  EXPECT_EQ(sql_pipeline5.transaction_context(), transaction_context);
  EXPECT_EQ(sql_pipeline5.get_sql_string(), _select_query_a);

  auto sql_pipeline6 = SQL{_select_query_a}
                           .set_optimizer(optimizer)
                           .set_prepared_statement_cache(prepared_cache)
                           .set_transaction_context(transaction_context)
                           .pipeline_statement();
  EXPECT_EQ(sql_pipeline6.transaction_context(), transaction_context);
  EXPECT_EQ(sql_pipeline6.get_sql_string(), _select_query_a);
}

TEST_F(SQLPipelineStatementTest, GetParsedSQL) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();
  const auto& parsed_sql = sql_pipeline.get_parsed_sql_statement();

  EXPECT_TRUE(parsed_sql->isValid());

  auto statements = parsed_sql->getStatements();

  EXPECT_EQ(statements.size(), 1u);
  EXPECT_EQ(statements.at(0)->type(), hsql::StatementType::kStmtSelect);
}

TEST_F(SQLPipelineStatementTest, GetParsedSQLMulti) {
  auto sql_pipeline = SQL{_multi_statement_query}.pipeline_statement();
  EXPECT_THROW(sql_pipeline.get_parsed_sql_statement(), std::exception);
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQP) {
  auto sql_pipeline = SQL{_join_query}.pipeline_statement();

  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  EXPECT_EQ(lqp->output_column_names(), _join_column_names);
  EXPECT_TRUE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPTwice) {
  auto sql_pipeline = SQL{_join_query}.pipeline_statement();

  sql_pipeline.get_unoptimized_logical_plan();
  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  EXPECT_EQ(lqp->output_column_names(), _join_column_names);
  EXPECT_TRUE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPValidated) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();

  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_TRUE(lqp->subplan_is_validated());
}

TEST_F(SQLPipelineStatementTest, GetUnoptimizedLQPNotValidated) {
  auto sql_pipeline = SQL{_select_query_a}.disable_mvcc().pipeline_statement();

  const auto& lqp = sql_pipeline.get_unoptimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_FALSE(lqp->subplan_is_validated());
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQP) {
  auto sql_pipeline = SQL{_join_query}.pipeline_statement();

  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  EXPECT_EQ(lqp->output_column_names(), _join_column_names);
  EXPECT_FALSE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPTwice) {
  auto sql_pipeline = SQL{_join_query}.pipeline_statement();

  sql_pipeline.get_optimized_logical_plan();
  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  EXPECT_EQ(lqp->output_column_names(), _join_column_names);
  EXPECT_FALSE(contained_in_lqp(lqp, contains_cross));
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPValidated) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();

  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_TRUE(lqp->subplan_is_validated());
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPNotValidated) {
  auto sql_pipeline = SQL{_select_query_a}.disable_mvcc().pipeline_statement();

  const auto& lqp = sql_pipeline.get_optimized_logical_plan();

  // We did not need the context yet
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
  EXPECT_FALSE(lqp->subplan_is_validated());
}

TEST_F(SQLPipelineStatementTest, GetOptimizedLQPDoesNotInfluenceUnoptimizedLQP) {
  auto sql_pipeline = SQL{_join_query}.pipeline_statement();

  const auto& unoptimized_lqp = sql_pipeline.get_unoptimized_logical_plan();

  // The optimizer works on the original LQP nodes which could be modified during optimization.
  // Copy the structure to check that it is equal after optimizing.
  std::shared_ptr<AbstractLQPNode> unoptimized_copy = unoptimized_lqp->deep_copy();

  // Optimize the LQP node
  sql_pipeline.get_optimized_logical_plan();
  const auto& unoptimized_lqp_new = sql_pipeline.get_unoptimized_logical_plan();

  EXPECT_LQP_EQ(unoptimized_copy, unoptimized_lqp_new);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlan) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();

  // We don't have a transaction context yet, as it was not needed
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);

  const auto& plan = sql_pipeline.get_query_plan();
  const auto& roots = plan->tree_roots();

  // We need the transaction context for the query plan if we use MVCC
  EXPECT_NE(sql_pipeline.transaction_context(), nullptr);
  EXPECT_EQ(roots.size(), 1u);
  EXPECT_NE(roots.at(0), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanTwice) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();

  sql_pipeline.get_query_plan();
  auto duration = sql_pipeline.compile_time_microseconds();

  const auto& plan = sql_pipeline.get_query_plan();
  auto duration2 = sql_pipeline.compile_time_microseconds();

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);

  const auto& roots = plan->tree_roots();

  EXPECT_EQ(roots.size(), 1u);
  EXPECT_NE(roots.at(0), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanJoinWithFilter) {
  auto sql_pipeline = SQL{_join_query}.pipeline_statement();

  const auto& plan = sql_pipeline.get_query_plan();
  const auto& roots = plan->tree_roots();

  auto is_join_op = [](const std::shared_ptr<const AbstractOperator>& node) {
    return static_cast<bool>(std::dynamic_pointer_cast<const AbstractJoinOperator>(node));
  };

  EXPECT_EQ(roots.size(), 1u);
  EXPECT_NE(roots.at(0), nullptr);
  EXPECT_TRUE(contained_in_query_plan(roots.at(0), is_join_op));
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanWithMVCC) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();
  const auto& plan = sql_pipeline.get_query_plan();

  EXPECT_NE(plan->tree_roots().at(0)->transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanWithoutMVCC) {
  auto sql_pipeline = SQL{_select_query_a}.disable_mvcc().pipeline_statement();
  const auto& plan = sql_pipeline.get_query_plan();

  EXPECT_EQ(plan->tree_roots().at(0)->transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetQueryPlanWithCustomTransactionContext) {
  auto context = TransactionManager::get().new_transaction_context();
  auto sql_pipeline = SQL{_select_query_a}.set_transaction_context(context).pipeline_statement();
  const auto& plan = sql_pipeline.get_query_plan();

  EXPECT_EQ(plan->tree_roots().at(0)->transaction_context().get(), context.get());
}

TEST_F(SQLPipelineStatementTest, GetTasks) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();

  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 3u);
  EXPECT_TRUE(_contains_validate(tasks));
}

TEST_F(SQLPipelineStatementTest, GetTasksTwice) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();

  sql_pipeline.get_tasks();
  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 3u);
  EXPECT_TRUE(_contains_validate(tasks));
}

TEST_F(SQLPipelineStatementTest, GetTasksNotValidated) {
  auto sql_pipeline = SQL{_select_query_a}.disable_mvcc().pipeline_statement();

  const auto& tasks = sql_pipeline.get_tasks();

  EXPECT_EQ(tasks.size(), 2u);
  EXPECT_FALSE(_contains_validate(tasks));
}

TEST_F(SQLPipelineStatementTest, GetResultTable) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineStatementTest, GetResultTableTwice) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();

  sql_pipeline.get_result_table();
  auto duration = sql_pipeline.execution_time_microseconds();

  const auto& table = sql_pipeline.get_result_table();
  auto duration2 = sql_pipeline.execution_time_microseconds();

  // Make sure this was not run twice
  EXPECT_EQ(duration, duration2);
  EXPECT_TABLE_EQ_UNORDERED(table, _table_a)
}

TEST_F(SQLPipelineStatementTest, GetResultTableJoin) {
  auto sql_pipeline = SQL{_join_query}.pipeline_statement();
  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineStatementTest, GetResultTableWithScheduler) {
//  auto sql_pipeline = SQL{_join_query}.pipeline_statement();

//  CurrentScheduler::set(std::make_shared<NodeQueueScheduler>(Topology::create_fake_numa_topology(8, 4)));
//  const auto& table = sql_pipeline.get_result_table();

//  EXPECT_TABLE_EQ_UNORDERED(table, _join_result);
}

TEST_F(SQLPipelineStatementTest, GetResultTableBadQueryNoMVCC) {
  auto sql = "SELECT a + b FROM table_a";
  auto sql_pipeline = SQL{sql}.disable_mvcc().pipeline_statement();

  // Make sure this is actually the failed execution and not a logic_error from the transaction management.
  EXPECT_THROW(sql_pipeline.get_result_table(), std::runtime_error);
}

TEST_F(SQLPipelineStatementTest, GetResultTableBadQuery) {
  auto sql = "SELECT a + b FROM table_a";
  auto sql_pipeline = SQL{sql}.pipeline_statement();

  EXPECT_THROW(sql_pipeline.get_result_table(), std::exception);
  EXPECT_TRUE(sql_pipeline.transaction_context()->aborted());
}

TEST_F(SQLPipelineStatementTest, GetResultTableNoOutput) {
  const auto sql = "UPDATE table_a SET a = 1 WHERE a < 5";
  auto sql_pipeline = SQL{sql}.pipeline_statement();

  const auto& table = sql_pipeline.get_result_table();
  EXPECT_EQ(table, nullptr);

  // Check that this doesn't crash. This should return the previous table, otherwise the auto-commit will fail.
  const auto& table2 = sql_pipeline.get_result_table();
  EXPECT_EQ(table2, nullptr);
}

TEST_F(SQLPipelineStatementTest, GetResultTableNoMVCC) {
  auto sql_pipeline = SQL{_select_query_a}.disable_mvcc().pipeline_statement();

  const auto& table = sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a);

  // Check that there really is no transaction management
  EXPECT_EQ(sql_pipeline.transaction_context(), nullptr);
}

TEST_F(SQLPipelineStatementTest, GetTimes) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();

  EXPECT_THROW(sql_pipeline.compile_time_microseconds(), std::exception);
  EXPECT_THROW(sql_pipeline.execution_time_microseconds(), std::exception);

  // Run to get times
  sql_pipeline.get_result_table();

  EXPECT_GT(sql_pipeline.compile_time_microseconds().count(), 0);
  EXPECT_GT(sql_pipeline.execution_time_microseconds().count(), 0);
}

TEST_F(SQLPipelineStatementTest, ParseErrorDebugMessage) {
#if !IS_DEBUG
  return;
#endif

  auto sql_pipeline = SQL{_invalid_sql}.pipeline_statement();
  try {
    sql_pipeline.get_parsed_sql_statement();
    // Fail if the previous command did not throw an exception
    FAIL();
  } catch (const std::exception& e) {
    const auto error_msg = std::string(e.what());
    // Check that the ^ was actually inserted in the error message
    EXPECT_TRUE(error_msg.find('^') != std::string::npos);
  }
}

TEST_F(SQLPipelineStatementTest, PreparedStatementPrepare) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement = "PREPARE x1 FROM 'SELECT * FROM table_a WHERE a = ?'";
  auto sql_pipeline =
      SQL{prepared_statement}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();

  sql_pipeline.get_query_plan();

  EXPECT_EQ(prepared_statement_cache->size(), 1u);
  EXPECT_TRUE(prepared_statement_cache->has("x1"));

  EXPECT_NO_THROW(sql_pipeline.get_result_table());
}

TEST_F(SQLPipelineStatementTest, PreparedStatementExecute) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement = "PREPARE x1 FROM 'SELECT * FROM table_a WHERE a = ?'";
  auto prepare_sql_pipeline =
      SQL{prepared_statement}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  prepare_sql_pipeline.get_result_table();

  EXPECT_EQ(prepared_statement_cache->size(), 1u);

  const std::string execute_statement = "EXECUTE x1 (123)";
  auto execute_sql_pipeline =
      SQL{execute_statement}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  const auto& table = execute_sql_pipeline.get_result_table();

  auto expected = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected->append({123, 456.7f});

  EXPECT_TABLE_EQ_UNORDERED(table, expected);
}

TEST_F(SQLPipelineStatementTest, PreparedStatementMultiPlaceholderExecute) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement = "PREPARE x1 FROM 'SELECT * FROM table_a WHERE a = ? OR (a > ? AND b < ?)'";
  auto prepare_sql_pipeline =
      SQL{prepared_statement}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  prepare_sql_pipeline.get_result_table();

  EXPECT_EQ(prepared_statement_cache->size(), 1u);

  const std::string execute_statement = "EXECUTE x1 (123, 10000, 500)";
  auto execute_sql_pipeline =
      SQL{execute_statement}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  const auto& table = execute_sql_pipeline.get_result_table();

  auto expected = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected->append({123, 456.7f});
  expected->append({12345, 458.7f});

  EXPECT_TABLE_EQ_UNORDERED(table, expected);
}

TEST_F(SQLPipelineStatementTest, MultiplePreparedStatementsExecute) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement1 = "PREPARE x1 FROM 'SELECT * FROM table_a WHERE a = ?'";
  const std::string prepared_statement2 = "PREPARE x2 FROM 'SELECT * FROM table_a WHERE a > ?'";
  const std::string prepared_statement_multi =
      "PREPARE x_multi FROM 'SELECT * FROM table_a WHERE a = ? OR (a > ? AND b < ?)'";

  auto prepare_sql_pipeline1 =
      SQL{prepared_statement1}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  auto prepare_sql_pipeline2 =
      SQL{prepared_statement2}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  auto prepare_sql_pipeline_multi =
      SQL{prepared_statement_multi}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();

  prepare_sql_pipeline1.get_result_table();
  prepare_sql_pipeline2.get_result_table();
  prepare_sql_pipeline_multi.get_result_table();

  EXPECT_EQ(prepared_statement_cache->size(), 3u);
  EXPECT_TRUE(prepared_statement_cache->has("x1"));
  EXPECT_TRUE(prepared_statement_cache->has("x2"));
  EXPECT_TRUE(prepared_statement_cache->has("x_multi"));

  const std::string execute_statement1 = "EXECUTE x1 (123)";
  const std::string execute_statement1_invalid = "EXECUTE x1 (123, 10000)";  // too many arguments

  const std::string execute_statement2 = "EXECUTE x2 (10000)";
  const std::string execute_statement2_invalid = "EXECUTE x2";  // too few arguments

  const std::string execute_statement_multi = "EXECUTE x_multi (123, 10000, 500)";
  const std::string execute_statement_multi_invalid = "EXECUTE x_multi (123, 10000, 500, 100)";  // too many arguments

  EXPECT_THROW(SQL(execute_statement1_invalid).pipeline_statement().get_result_table(), std::runtime_error);
  EXPECT_THROW(SQL(execute_statement2_invalid).pipeline_statement().get_result_table(), std::runtime_error);
  EXPECT_THROW(SQL(execute_statement_multi_invalid).pipeline_statement().get_result_table(), std::runtime_error);

  auto execute_sql_pipeline1 =
      SQL{execute_statement1}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  const auto& table1 = execute_sql_pipeline1.get_result_table();

  auto execute_sql_pipeline2 =
      SQL{execute_statement2}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  const auto& table2 = execute_sql_pipeline2.get_result_table();

  auto execute_sql_pipeline_multi =
      SQL{execute_statement_multi}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  const auto& table_multi = execute_sql_pipeline_multi.get_result_table();

  // x1 result
  auto expected1 = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected1->append({123, 456.7f});

  // x2 result
  auto expected2 = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected2->append({12345, 458.7f});

  // x_multi result
  auto expected_multi = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected_multi->append({123, 456.7f});
  expected_multi->append({12345, 458.7f});

  EXPECT_TABLE_EQ_UNORDERED(table1, expected1);
  EXPECT_TABLE_EQ_UNORDERED(table2, expected2);
  EXPECT_TABLE_EQ_UNORDERED(table_multi, expected_multi);
}

TEST_F(SQLPipelineStatementTest, PreparedInsertStatementExecute) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement = "PREPARE x1 FROM 'INSERT INTO table_a VALUES (?, ?)'";
  auto prepare_sql_pipeline =
      SQL{prepared_statement}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  prepare_sql_pipeline.get_result_table();

  EXPECT_EQ(prepared_statement_cache->size(), 1u);

  const std::string execute_statement = "EXECUTE x1 (1, 0.75)";
  auto execute_sql_pipeline =
      SQL{execute_statement}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  execute_sql_pipeline.get_result_table();

  auto select_sql_pipeline = SQL{_select_query_a}.pipeline_statement();
  const auto table = select_sql_pipeline.get_result_table();

  EXPECT_TABLE_EQ_UNORDERED(table, _table_a);
}

TEST_F(SQLPipelineStatementTest, PreparedUpdateStatementExecute) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement = "PREPARE x1 FROM 'UPDATE table_a SET a = ? WHERE a = ?'";
  auto prepare_sql_pipeline =
      SQL{prepared_statement}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  prepare_sql_pipeline.get_result_table();

  EXPECT_EQ(prepared_statement_cache->size(), 1u);

  const std::string execute_statement = "EXECUTE x1 (1, 123)";
  auto execute_sql_pipeline =
      SQL{execute_statement}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  execute_sql_pipeline.get_result_table();

  auto select_sql_pipeline = SQL{_select_query_a}.pipeline_statement();
  const auto table = select_sql_pipeline.get_result_table();

  auto expected = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected->append({1, 456.7f});
  expected->append({1234, 457.7f});
  expected->append({12345, 458.7f});

  EXPECT_TABLE_EQ_UNORDERED(table, expected);
}

TEST_F(SQLPipelineStatementTest, PreparedDeleteStatementExecute) {
  auto prepared_statement_cache = std::make_shared<SQLQueryCache<SQLQueryPlan>>(5);

  const std::string prepared_statement = "PREPARE x1 FROM 'DELETE FROM table_a WHERE a = ?'";
  auto prepare_sql_pipeline =
      SQL{prepared_statement}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  prepare_sql_pipeline.get_result_table();

  EXPECT_EQ(prepared_statement_cache->size(), 1u);

  const std::string execute_statement = "EXECUTE x1 (123)";
  auto execute_sql_pipeline =
      SQL{execute_statement}.set_prepared_statement_cache(prepared_statement_cache).pipeline_statement();
  execute_sql_pipeline.get_result_table();

  auto select_sql_pipeline = SQL{_select_query_a}.pipeline_statement();
  const auto table = select_sql_pipeline.get_result_table();

  auto expected = std::make_shared<Table>(_int_float_column_definitions, TableType::Data);
  expected->append({1234, 457.7f});
  expected->append({12345, 458.7f});

  EXPECT_TABLE_EQ_UNORDERED(table, expected);
}

TEST_F(SQLPipelineStatementTest, CacheQueryPlan) {
  auto sql_pipeline = SQL{_select_query_a}.pipeline_statement();
  sql_pipeline.get_result_table();

  const auto& cache = SQLQueryCache<SQLQueryPlan>::get();
  EXPECT_EQ(cache.size(), 1u);
  EXPECT_TRUE(cache.has(_select_query_a));
}

}  // namespace opossum
