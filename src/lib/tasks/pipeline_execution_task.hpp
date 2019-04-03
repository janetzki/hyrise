#pragma once

#include <memory>
#include <string>
#include "scheduler/abstract_task.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

class SQLPipeline;

class PipelineExecutionTask : public AbstractTask {
 public:
  explicit PipelineExecutionTask(SQLPipelineBuilder builder);

  std::shared_ptr<SQLPipeline> get_sql_pipeline();

  void set_query_done_callback(const std::function<void()>& done_callback);

 protected:
  void _on_execute() override;

 private:
  SQLPipelineBuilder _builder;
  // std::vector<std::shared_ptr<AbstractTask>> query_tasks;
  std::shared_ptr<AbstractTask> last_task;
  std::shared_ptr<SQLPipeline> _sql_pipeline;
  std::function<void()> _done_callback;
};
}  // namespace opossum
