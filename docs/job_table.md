## job_table

| 表项           | 功能               |
| -------------- | ------------------ |
| job_db_index   | 主键               |
| mod_time       | 修改时间           |
| deleted        | 是否已经删除       |
| account        | 账户名称           |
| cpus_req       | 需要的cpu数量      |
| mem_req        | 需要的内存数量     |
| env_vars       | 环境变量           |
| flags          | 任务flags          |
| job_name       | 任务名称           |
| id_assoc       | 暂时不用           |
| id_job         | 任务id             |
| id_user        | 用户id             |
| id_group       | 用户组id           |
| nodelist       | 节点列表           |
| nodes_alloc    | 分配节点数量       |
| node_inx       | 分配节点编号       |
| partition      | 分区               |
| priority       | 优先级             |
| state          | 任务状态           |
| timelimit      | 时间限制           |
| time_submit    | 提交时间           |
| time_start     | 任务开始时间       |
| time_end       | 任务结束时间       |
| time_suspended | 任务挂起时间       |
| work_dir       | 工作目录           |
| submit_line    | 提交的命令         |
| tres_alloc     | 需要追踪的资源信息 |
| tres_req       | 请求追踪的资源信息 |

## cluster_table

| 表项          | 功能                 |
| ------------- | -------------------- |
| creation_time | 创建时间             |
| mod_time      | 修改时间             |
| deleted       | 是否已经删除         |
| name          | 集群名字             |
| control_host  | 控制器ip             |
| control_port  | 控制器端口           |
| last_port     | 上次使用的控制器端口 |
| rpc_version   | rpc版本              |

## acct_table

| 表项          | 功能         |
| ------------- | ------------ |
| creation_time | 创建时间     |
| mod_time      | 修改时间     |
| deleted       | 是否已经删除 |
| name          | 账户名称     |
| description   | 描述         |
| organization  | 组织         |

## user_table

| 表项          | 功能         |
| ------------- | ------------ |
| creation_time | 创建时间     |
| mod_time      | 修改时间     |
| deleted       | 是否删除     |
| name          | 名称         |
| admin_level   | 是否为管理员 |

## assoc_table

| 表项          | 功能                   |
| ------------- | ---------------------- |
| creation_time | 创建时间               |
| mod_time      | 修改时间               |
| deleted       | 是否删除               |
| is_def        | 是否为某账户的定义     |
| id_assoc      | （主键）association id |
| user          | 用户                   |
| acct          | 账户                   |
| partition     | 分区                   |
| parent_acct   | 父账户                 |
| shares        | 资源分配比重           |

