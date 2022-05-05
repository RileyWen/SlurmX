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
        if (!ParseNodeList(head_str, nodelist,
                           fmt::format("{}{}{}", node_num[i], end_, end_str)))
          nodelist->push_back(
              fmt::format("{}{}{}{}", head_str, node_num[i], end_, end_str));
      } else if (std::regex_match(node_num[i], std::regex(R"(^\d+-\d+$)"))) {
        std::vector<std::string> loc_index;
        boost::split(loc_index, node_num[i], boost::is_any_of("-"));
        size_t len = loc_index[0].length();
        for (size_t j = std::stoi(loc_index[0]); j <= std::stoi(loc_index[1]);
             j++) {
          std::string s_num = fmt::format("{:0>{}}", std::to_string(j), len);
          if (!ParseNodeList(head_str, nodelist,
                             fmt::format("{}{}{}", s_num, end_, end_str))) {
            nodelist->push_back(
                fmt::format("{}{}{}{}", head_str, s_num, end_, end_str));
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

std::string HostNameListToStr(const std::list<std::string> &hostlist) {
  std::map<std::string, std::list<std::string>> host_map;
  std::string host_name_str;

  if (hostlist.empty()) return host_name_str;

  for (const auto &host : hostlist) {
    if (host.empty()) continue;
    std::regex regex(R"(\d+$)");
    std::smatch match;
    if (std::regex_search(host, match, regex)) {
      std::string num_str = match[0],
                  head_str = host.substr(0, match.position(0));
      auto iter = host_map.find(head_str);
      if (iter == host_map.end()) {
        std::list<std::string> list;
        host_map[head_str] = list;
      }
      host_map[head_str].push_back(num_str);
    } else {
      host_name_str += host;
      host_name_str += ",";
    }
  }

  for (auto &&iter : host_map) {
    if (iter.second.size() == 1) {
      host_name_str += iter.first;
      host_name_str += iter.second.front();
      host_name_str += ",";
      continue;
    }
    host_name_str += iter.first;
    host_name_str += "[";
    iter.second.sort();
    iter.second.unique();
    int first = -1, last = -1, num;
    std::string first_str, last_str;
    for (const auto &num_str : iter.second) {
      num = stoi(num_str);
      if (first < 0) {  // init the head
        first = last = num;
        first_str = last_str = num_str;
        continue;
      }
      if (num == last + 1) {  // update the tail
        last++;
        last_str = num_str;
      } else {
        if (first == last) {
          host_name_str += first_str;
        } else {
          host_name_str += first_str;
          host_name_str += "-";
          host_name_str += last_str;
        }
        host_name_str += ",";
        first = last = num;
        first_str = last_str = num_str;
      }
    }
    if (first == last) {
      host_name_str += first_str;
    } else {
      host_name_str += first_str;
      host_name_str += "-";
      host_name_str += last_str;
    }
    host_name_str += "],";
  }
  host_name_str.pop_back();
  return host_name_str;
}

}  // namespace util
