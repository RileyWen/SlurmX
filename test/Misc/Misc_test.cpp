#include <absl/container/btree_map.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

TEST(Misc, AbseilBtreeMap) {
  absl::btree_map<int, int> map;
  map.emplace(1, 2);
  map.emplace(2, 3);
  map.emplace(3, 4);

  auto iter = map.begin();
  for (int i = 1; i <= 3; i++, iter++) {
    EXPECT_EQ(iter->first, i);
  }

  iter = map.find(3);
  map.emplace(4, 5);
  EXPECT_EQ((++iter)->first, 4);
  map.emplace(5, 6);
  EXPECT_EQ((++iter)->second, 6);
}
