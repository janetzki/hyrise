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

 protected:
  void _on_execute() override;

 private:
  SQLPipelineBuilder _builder;
  std::shared_ptr<SQLPipeline> _sql_pipeline;
};
}  // namespace opossum
