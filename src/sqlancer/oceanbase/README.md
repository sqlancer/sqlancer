## Install Oceanbase
There are some methods to install OceanBase.
A method to install a local single-node OceanBase cluster:
```shell
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://mirrors.aliyun.com/oceanbase/OceanBase.repo
sudo yum install -y ob-deploy
source /etc/profile.d/obd.sh

git clone https://github.com/oceanbase/obdeploy.git
cd obdeploy 
sudo obd cluster deploy c1 -c ./example/mini-local-example.yaml -A
sudo obd cluster start c1
```
> **NOTE:** We do not recommend that you use sys tenant to test. So please deploy clutser with optition -A, will create the test tenant during the bootstrap by using all available resources of the cluster.
> **NOTE:** Then you can create user.

```shell
mysql -h127.1 -uroot@test -P2881 -Doceanbase -A -e"create user sqlancer identified by 'sqlancer';grant all on *.* to sqlancer;"
```
Other methods, see [OceanBase Deployer](https://github.com/oceanbase/obdeploy/blob/master/README.md).
