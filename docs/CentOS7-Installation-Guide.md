# centos7 Crane环境配置

以下内容为配置代码编译环境，在编译项目的节点执行

## 1.环境准备

安装ntp ntpdate同步时钟

```shell
yum install ntp ntpdate
systemctl start ntpd
systemctl enable ntpd

timedatectl set-timezone Asia/Shanghai
```

关闭防火墙，不允许关闭防火墙则考虑开放10011、10010、873端口
```shell
systemctl stop firewalld
systemctl disable firewalld

# 或者开放端口
firewall-cmd --add-port=10011/tcp --permanent --zone=public
firewall-cmd --add-port=10010/tcp --permanent --zone=public
firewall-cmd --add-port=873/tcp --permanent --zone=public
# 重启防火墙(修改配置后要重启防火墙)
firewall-cmd --reload
```

## 2.安装依赖包

```shell
yum install -y epel-release pv openssl-devel libcgroup-devel curl-devel boost169-devel boost169-static
```

## 3.安装工具链

安装C++11

```shell
# Install CentOS SCLo RH repository:
yum install centos-release-scl-rh
# Install devtoolset-11 rpm package:
yum install devtoolset-11
# 第三步就是使新的工具集生效
scl enable devtoolset-11 bash
```
这时用gcc --version查询，可以看到版本已经是11.2系列了

```shell
$ gcc --version
gcc (GCC) 11.2.1 20220127 (Red Hat 11.2.1-9)
Copyright (C) 2021 Free Software Foundation, Inc.
This is free software; see the source for copying conditions.  There is NO
warranty; not even for MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
```

为了避免每次手动生效，可以在.bashrc中设置，此文件中修改对当前用户永久生效

```shell
vim ~/.bashrc

# 在最后一行加上
source /opt/rh/devtoolset-11/enable
or
source scl_source enable devtoolset-11

# 使环境变量生效
source ~/.bashrc 
```

安装cmake和ninja

选择一个合适的源码存放位置，从github下载源码
```shell
wget https://github.com/ninja-build/ninja/releases/download/v1.10.2/ninja-linux.zip
wget https://github.com/Kitware/CMake/releases/download/v3.21.3/cmake-3.21.3.tar.gz
```

解压编译安装，首次编译时间较长，请耐心等待
```shell
unzip ninja-linux.zip
cp ninja /usr/local/bin/

tar -zxvf cmake-3.21.3.tar.gz
cd cmake-3.21.3
./bootstrap
gmake
gmake install
```

检查安装是否成功
```shell
cmake --version
#cmake version 3.21.3
#
#CMake suite maintained and supported by Kitware (kitware.com/cmake).
```

报错

```shell
CMake Error: Could not find CMAKE_ROOT !!!
CMake has most likely not been installed correctly.
Modules directory not found in
/usr/local/bin
Segmentation fault
```

出现这种情况一般情况下是因为我们在安装cmake之前执行过cmake命令，终端的哈希表会记录下执行过的命令的路径，相当于缓存。第一次执行命令shell解释器默认的会从PATH路径下寻找该命令的路径，当我们第二次使用该命令时，shell解释器首先会查看哈希表，没有该命令才会去PATH路径下寻找。

所以哈希表可以大大提高命令的调用速率，但是CMake Error: Could not find
CMAKE_ROOT错误的原因也出在这里，如果我们之前在这个终端执行过cmake命令，那么哈希表就会自动记录下之前版本cmake的路径，我们可以通过输入hash -l查看，如下所示：

```shell
[root@cn17 cmake-3.21.3]# hash -l
builtin hash -p /usr/bin/wget wget
builtin hash -p /usr/bin/cmake cmake
```

所以当我们更新了cmake以后，当我们输入cmake相关命令时，shell解释器便会去哈希表里面查找之前版本cmake的路径，然后便产生了错误。

此时我们可以重新开一个终端，也可以在该终端执行hash -r命令来清除哈希表的内容，然后再执行cmake --version命令。

## 4.安装mongodb

安装数据库仅在需要存储数据的节点安装

```shell
# 下载并解压安装包
wget https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-rhel70-5.0.9.tgz
tar -zxvf mongodb-linux-x86_64-rhel70-5.0.9.tgz
# 重命名
mv mongodb-linux-x86_64-rhel70-5.0.9  /opt/mongodb
# 添加环境变量  
vim /etc/profile
```

在配置文件中添加如下内容（路径应对应mongodb安装路径）
```shell
export MONGODB_HOME=/opt/mongodb
export PATH=$PATH:${MONGODB_HOME}/bin
```

```shell
# 使环境变量生效
source /etc/profile 
# 创建db目录和log目录
cd /opt/mongodb
mkdir -p ./data/db
mkdir -p ./logs
touch ./logs/mongodb.log
```

创建mongodb.conf配置文件，内容如下：

```shell
vim mongodb.conf

#端口号
port=27017
#db目录
dbpath=/opt/mongodb/data/db
#日志目录
logpath=/opt/mongodb/logs/mongodb.log
#后台
fork=true
#日志输出
logappend=true
#允许远程IP连接
bind_ip=0.0.0.0
#开启权限验证
#auth=true
```

启动测试
```shell
mongod --config /opt/mongodb/mongodb.conf
mongo
```

创建用户

```shell
use admin
db.createUser({
  user:'admin',  # 用户名
  pwd:'123456',  # 密码
  roles:[{ role:'root',db:'admin'}]   #root 代表超級管理员权限 admin代表给admin数据库加的超级管理员
})

db.shutdownServer() # 重启前先关闭服务器
```

修改/opt/mongodb/mongodb.conf配置文件，将权限验证的注释放开

```shell
vim /opt/mongodb/mongodb.conf

......
#开启权限验证
auth=true
```

重新启动mongodb数据库

```shell
mongod --config /opt/mongodb/mongodb.conf
```

编辑开机启动

```shell
vi /etc/rc.local
# 加入如下语句，以便启动时执行：
mongod --config /opt/mongodb/mongodb.conf
```

## 4*.安装mariadb(数据库移植代码稳定后此步骤可以删除)

通过yum安装就行了，安装mariadb-server，默认依赖安装mariadb，一个是服务端、一个是客户端。

```shell
yum install MariaDB-server

systemctl start mariadb  # 开启服务
systemctl enable mariadb  # 设置为开机自启动服务

mariadb-secure-installation  # 首次安装需要进行数据库的配置
```

配置时出现的各个选项

```shell
Enter current password for root (enter for none):  # 输入数据库超级管理员root的密码(注意不是系统root的密码)，第一次进入还没有设置密码则直接回车

Set root password? [Y/n]  # 设置密码，y

New password:  # 新密码  123456
Re-enter new password:  # 再次输入密码:123456

Remove anonymous users? [Y/n]  # 移除匿名用户， y

Disallow root login remotely? [Y/n]  # 拒绝root远程登录，n，不管y/n，都会拒绝root远程登录

Remove test database and access to it? [Y/n]  # 删除test数据库，y：删除。n：不删除，数据库中会有一个test数据库，一般不需要

Reload privilege tables now? [Y/n]  # 重新加载权限表，y。或者重启服务也许
```

安装过程中遇到Table doesn’t exist报错：
ERROR 1146 (42S02) at line 1: Table ‘mysql.global_priv’ doesn’t exist … Failed!
执行下面的命令后，重启mysql

```shell
mysql_upgrade -uroot -p --force
```

重启数据库

```shell
systemctl restart mariadb
```

登陆数据库

```shell
mysql -uroot -p
Enter password:
Welcome to the MariaDB monitor.  Commands end with ; or \g.
Your MariaDB connection id is 9
Server version: 10.4.8-MariaDB MariaDB Server
```

进入mysql数据库
```shell
use mysql
```

查询user表，可看到多条数据
```shell
select host,user,password from user;
```

删除localhost以外数据

```shell
delete from user where host !='localhost';
```

配置完毕，退出

```shell
exit;
systemctl restart mariadb
```

## 5.编译Crane程序

```shell
# 由于便于项目克隆git仓库，可以先设置好git代理
git config --global http.proxy http://<ip>:<port>
git config --global http.proxy http://<ip>:<port>

# 选择一个合适的位置克隆项目
git clone -b dev/fix_link_flags https://github.com/PKUHPC/Crane.git

cd Crane
mkdir build
cd build/

# 首次编译需要下载第三方库，耗时较长
cmake -G Ninja -DCMAKE_C_COMPILER=/opt/rh/devtoolset-11/root/usr/bin/gcc -DCMAKE_CXX_COMPILER=/opt/rh/devtoolset-11/root/usr/bin/g++ ..
ninja cranectld
ninja craned
```

## 6.Pam模块(待完善)

首次编译完成后需要将pam模块动态链接库放入系统指定位置

```shell
cp Crane/build/src/Misc/Pam/pam_Crane.so /usr/lib64/security/
```

同时计算节点“/etc/security/access.conf”文件禁止非root用户登录

Required pam_access.so
