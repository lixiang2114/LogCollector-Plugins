### 插件库说明
1. 本库中的所有插件是基于LogCollector-2.0版本开发的，对于LogCollector系统框架而言，具有向后兼容性  
2. 所有插件的使用说明请参考本库中对应插件目录下的README.md文档
3. 插件目录说明：
|文件或目录名|文件或目录介绍|文件或目录备注说明|
|:-----:|:-------:|:-------:|
|src|插件源码目录|可以自行依赖源码构建插件核心库|
|dst|插件二进制安装包|使用时将插件安装包解压到LogCollector安装目录下的plugins目录下即可|
|target|插件编译打包目录|依赖于src源码的编译打包目录，目录中库文件对应插件安装目录下lib目录中的核心库|
|pom.xml|插件编译配置文件|插件编译、打包的Maven配置文件，需严格按照LogCollector2.0系统框架来执行配置编排|
|README.md|插件使用说明文档|详细介绍插件的开发背景、插件的功能特性、插件的下载、安装、部署、配置及使用说明等|