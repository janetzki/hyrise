#include "pipeline_execution_task.hpp"

#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

PipelineExecutionTask::PipelineExecutionTask(SQLPipelineBuilder builder) : _builder(std::move(builder)) {}

std::shared_ptr<SQLPipeline> PipelineExecutionTask::get_sql_pipeline() {
  if (!(last_task->is_done())) {
    CurrentScheduler::wait_for_tasks(std::vector<std::shared_ptr<AbstractTask>>{last_task});
  }
  return _sql_pipeline;
}

void PipelineExecutionTask::set_query_done_callback(const std::function<void()>& done_callback) {
  _done_callback = done_callback;
}

void PipelineExecutionTask::_on_execute() {
  _sql_pipeline = std::make_shared<SQLPipeline>(_builder.create_pipeline());
  auto tasks_per_statement = _sql_pipeline->get_tasks();

  last_task = tasks_per_statement.back().back();
  last_task->set_done_callback(_done_callback);

  for (auto tasks : tasks_per_statement) {
    CurrentScheduler::schedule_tasks(tasks);
  }
}
}  // namespace opossum
