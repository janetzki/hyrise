#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "optimizer/abstract_syntax_tree/delete_node.hpp"

namespace opossum {

class DeleteNodeTest : public BaseTest {
 protected:
  void SetUp() override { _delete_node = std::make_shared<DeleteNode>("table_a"); }

  void TearDown() override {}

  std::shared_ptr<DeleteNode> _delete_node;
};

TEST_F(DeleteNodeTest, Description) { EXPECT_EQ(_delete_node->description(), "[Delete] Table: 'table_a'"); }

}  // namespace opossum
