#include "slurmx/String.h"


namespace util {

std::string ReadableMemory(uint64_t memory_bytes) {
  if (memory_bytes < 1024)
    return fmt::format("{}B", memory_bytes);
  else if (memory_bytes < 1024 * 1024)
    return fmt::format("{}K", memory_bytes / 1024);
  else if (memory_bytes < 1024 * 1024 * 1024)
    return fmt::format("{}M", memory_bytes / 1024 / 1024);
  else
    return fmt::format("{}G", memory_bytes / 1024 / 1024 / 1024);
}

bool ParseNodeList(const std::string &node_str,
                   std::list<std::string> *nodelist,
                   const std::string &end_str) {
  std::regex regex(R"(.*\[(.*)\])");
  std::smatch match;
  if (std::regex_search(node_str, match, regex)) {
    std::vector<std::string> node_num;
    std::string match_str = match[1],
                head_str = node_str.substr(0, match.position(1) - 1),
                end_ =
                    node_str.substr(match.position(1) + match_str.size() + 1);

    boost::split(node_num, match_str, boost::is_any_of(","));
    size_t len = node_num.size();
    for (size_t i = 0; i < len; i++) {
      if (std::regex_match(node_num[i], std::regex(R"(^\d+$)"))) {
        if (!ParseNodeList(head_str, nodelist, fmt::format("{}{}{}",node_num[i] , end_ , end_str)))
          nodelist->push_back(fmt::format("{}{}{}{}",head_str , node_num[i] , end_ , end_str));
      } else if (std::regex_match(node_num[i], std::regex(R"(^\d+-\d+$)"))) {
        std::vector<std::string> loc_index;
        boost::split(loc_index, node_num[i], boost::is_any_of("-"));
        for (size_t j = std::stoi(loc_index[0]); j <= std::stoi(loc_index[1]);
             j++) {
          if (!ParseNodeList(head_str, nodelist,
                             fmt::format("{}{}{}",j , end_ , end_str))) {
            nodelist->push_back(fmt::format("{}{}{}{}",head_str , j , end_ , end_str));
          }
        }
      } else {
        return false;
      }
    }
    return true;
  }
  return false;
}

bool ParseHostList(const std::string &host_str,
                   std::list<std::string> *hostlist) {
  std::regex regex(R"(.*\[(.*)\]$)");
  if (!std::regex_match(host_str, regex)) {
    hostlist->emplace_back(host_str);
    return true;
  }

  std::string end_str;
  return ParseNodeList(host_str, hostlist, end_str);
}

}

