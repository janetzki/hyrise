#include "pipeline_execution_task.hpp"

#include <chrono>
#include "scheduler/current_scheduler.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

PipelineExecutionTask::PipelineExecutionTask(const SQLPipelineBuilder builder) : _builder(std::move(builder)) {}

std::shared_ptr<SQLPipeline> PipelineExecutionTask::get_sql_pipeline() {
  if (!query_tasks.back()->is_done()) {
    CurrentScheduler::wait_for_tasks(query_tasks);
  }
  return _sql_pipeline;
}

void PipelineExecutionTask::set_query_done_callback(const std::function<void()>& done_callback) {
  _done_callback = done_callback;
}

std::vector<std::shared_ptr<AbstractTask>> PipelineExecutionTask::get_tasks() const {
  // Task might not have been executed so far
  while (!is_done()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return query_tasks;
}

void PipelineExecutionTask::_on_execute() {
  _sql_pipeline = std::make_shared<SQLPipeline>(_builder.create_pipeline());
  auto tasks_per_statement = _sql_pipeline->get_tasks();

  tasks_per_statement.back().back()->set_done_callback(_done_callback);

  for (auto tasks : tasks_per_statement) {
    CurrentScheduler::schedule_tasks(tasks);
    query_tasks.insert(query_tasks.end(), tasks.begin(), tasks.end());
  }
}
}  // namespace opossum
