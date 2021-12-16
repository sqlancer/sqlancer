## Install Oceanbase
There are some methods to install OceanBase.
A method to install a local single-node OceanBase cluster:
### Install OBD by using RPM packages (only for CentOS 7 or later)
```shell
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo
sudo yum install -y ob-deploy
source /etc/profile.d/obd.sh
```
### Start an OceanBase cluster
```shell
git clone https://github.com/oceanbase/obdeploy.git
cd obdeploy 
sudo obd cluster deploy c1 -c ./example/mini-local-example.yaml -A
sudo obd cluster start c1
```
After you install OBD, you can run these commands as the root user to start a local single-node OceanBase cluster.
Before you run the commands, make sure that these conditions are met:

- You have logged on as the root user.
- Ports `2881` and `2882` are available.
- Your server has at least 8 GB of memory.
- Your server has at least 2 CPU cores.

> **NOTE:** If the preceding conditions are not met, see [OceanBase Deployer](https://github.com/oceanbase/obdeploy/blob/master/README.md).

> **NOTE:** We do not recommend that you use sys tenant to test. So please deploy clutser with optition -A, will create the test tenant during the bootstrap by using all available resources of the cluster.

### Then create user for test.

```shell
mysql -h127.1 -uroot@test -P2881 -Doceanbase -A -e"create user sqlancer identified by 'sqlancer';grant all on *.* to sqlancer;"
```
Other methods, see [OceanBase Deployer](https://github.com/oceanbase/obdeploy/blob/master/README.md).
