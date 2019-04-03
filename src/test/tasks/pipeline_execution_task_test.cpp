#include "base_test.hpp"

#include "sql/sql_pipeline_builder.hpp"
#include "storage/storage_manager.hpp"
#include "tasks/pipeline_execution_task.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class PipelineExecutionTaskTest : public BaseTest {
 public:
  void SetUp() override {  // called ONCE before the tests
    int_float_tbl = load_table("resources/test_data/tbl/int_float.tbl");
    StorageManager::get().add_table("int_float_tbl", int_float_tbl);
  }
  std::shared_ptr<Table> int_float_tbl;
};

TEST_F(PipelineExecutionTaskTest, ExecuteSimpleQuery) {
  const std::string sql = "SELECT a FROM int_float_tbl";

  auto sql_pipeline_builder = SQLPipelineBuilder{sql};
  auto pipeline_task = std::make_shared<PipelineExecutionTask>(std::move(sql_pipeline_builder));

  pipeline_task->execute();

  EXPECT_FALSE(pipeline_task->get_sql_pipeline()->failed_pipeline_statement());
}

TEST_F(PipelineExecutionTaskTest, ExecuteBadQuery) {
  const std::string sql = "SELECT a + not_a_column FROM int_float_tbl";

  auto sql_pipeline_builder = SQLPipelineBuilder{sql};
  auto pipeline_task = std::make_shared<PipelineExecutionTask>(std::move(sql_pipeline_builder));

  EXPECT_THROW(pipeline_task->execute(), std::exception);
}
}  // namespace opossum
