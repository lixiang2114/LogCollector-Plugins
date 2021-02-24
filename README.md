### 安装目录介绍  
|目录名称|目录介绍|备注说明|
|:-----:|:-------:|:-------:|
|bin|二进制脚本及可执行文件|用于启停TRA服务/ETL服务、执行检查点等|
|sys|系统平台核心运行库路径|用以支撑不同系统平台的运行时环境，不能变动或改变|
|lib|系统应用核心支撑库路径|系统运行时依赖所必须，若没有特别理由，不要去改变|
|conf|系统上下文及其日志配置|全局服务参数、应用上下文参数和系统级日志配置参数|
|logs|系统应用运行时日志路径|若服务无法启动或系统启动错误，可通过系统日志进行排查|
|flows|系统流程实例运行时目录|一个流程对应一个目录，该目录中插件支持缓存和运行时生成|
|plugins|系统流程应用插件安装目录|插件服务于流程，可以通过自定义和安装插件来扩展本系统功能|
