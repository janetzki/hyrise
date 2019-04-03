#pragma once

#include <memory>
#include <string>
#include "scheduler/abstract_task.hpp"
#include "sql/sql_pipeline_builder.hpp"

namespace opossum {

class SQLPipeline;

class PipelineExecutionTask : public AbstractTask {
 public:
  explicit PipelineExecutionTask(const SQLPipelineBuilder builder);

  std::shared_ptr<SQLPipeline> get_sql_pipeline();

  void set_query_done_callback(const std::function<void()>& done_callback);

  std::vector<std::shared_ptr<AbstractTask>> get_tasks() const;

 protected:
  void _on_execute() override;

 private:
  const SQLPipelineBuilder _builder;
  std::vector<std::shared_ptr<AbstractTask>> query_tasks;
  std::shared_ptr<SQLPipeline> _sql_pipeline;
  std::function<void()> _done_callback;
};
}  // namespace opossum
