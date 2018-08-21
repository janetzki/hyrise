#pragma once

#include "joe_config.hpp"
#include "joe_query_iteration.hpp"

namespace opossum {

class Joe;

struct JoeQuerySample {
  std::string name;
  std::shared_ptr<JoePlan> best_plan;
};

std::ostream &operator<<(std::ostream &stream, const JoeQuerySample &sample);

class JoeQuery final {
 public:
  JoeQuery(const Joe& joe, const std::string& name, const std::string& sql);

  void run();

  void visualize_join_graph(const std::shared_ptr<JoinGraph>& join_graph);
  std::string prefix() const;

  const Joe& joe;
  std::string sql;

  std::chrono::steady_clock::time_point execution_begin;
  bool save_plan_results{false};
  bool join_graph_visualized{false};
  std::unordered_set<std::shared_ptr<AbstractLQPNode>, LQPHash, LQPEqual> executed_plans;

  std::vector<JoeQueryIteration> query_iterations;

  JoeQuerySample sample;
};

}  // namespace opossum