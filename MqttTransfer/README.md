### MQTT转存器  
MQTT转存器可以将来自基于MQTT协议消息中间件的实时数据主动推送并转存到缓冲文件中去，转存器与收集器的一个最大区别在于：收集器应用的数据源主要是存储或数据库，而转存器应用的数据源主要是来自日志文件、消息中间件或实时服务客户端。前者数据源中的数据是被转存器主动拉取，而后者数据源中的数据是被主动推送给转存器。  
如果转存缓冲文件尺寸超过阈值则按数字递增序列滚动，这区别于log4j的重命名方式。虽然转存器支持对下游ETL通道的推送实现，但是考虑到这总机制不利于数据安全，同时易引发通道OOM错误，故现有的转存器实现都是基于转存文件缓冲的，而非通道。  
​      

### 下载与安装  
wget https://github.com/lixiang2114/LogCollector-Plugins/raw/main/MqttTransfer/dst/mqttTransfer.zip -d /install/zip/  
unzip  /install/zip/mqttTransfer.zip -d /software/LogCollector-2.0/plugins/    

##### 备注：  
插件配置路径：  
 /software/LogCollector-2.0/plugins/mqttTransfer/transfer.properties  
​      

### 参数值介绍  
|参数名称|参数含义|缺省默认|备注说明|
|:-----:|:-------:|:-------:|:-------:|
|topic|连接主题|无|连接MQTT服务器的主题名称，该参数是必选参数|
|hostList|地址列表|127.0.0.1:1883|连接MQTT服务器的单点地址(单点)或地址列表(集群)|
|jwtSecret|Token秘钥|无|使用Token认证时的秘钥，是否可以匿名访问取决于服务器类型|
|passWord|登录密码|public|本插件登录到MQTT消息中间件服务器的认证用户密码|
|userName|登录用户|admin|本插件登录MQTT消息中间件服务器的认证用户名|
|tokenFrom|Token字段|password|本插件登录认证MQTT消息中间件服务时，携带token的字段|
|tokenExpire|Token时长|1h|本插件客户端持有的Token的过期时长，-1表示永不过期|
|protocolType|连接协议|tcp|本插件客户端连接MQTT消息中间件服务使用的传输层协议类型|
|transferSaveFile|转存文件|buffer.log.0|转存目标文件路径，转存文件超过阈值尺寸则按数字递增滚动|
|persistenceType|持久类型|MemoryPersistence|本插件客户端对MQTT消息中间件中消息的持久化类型|
|transferSaveMaxSize|转存尺寸|2GB|转存目标文件最大阈值尺寸，超过改尺寸按数值递增滚动|
##### 备注：  
1. MQTT服务器是否可以被匿名访问取决于服务器的类型，如果服务器支持则可以设置为免密登录模式。  
2. MemoryPersistence类的全类名是：org.eclipse.paho.client.mqttv3.persist.MemoryPersistence.MemoryPersistence，除了可以基于内存持久化，可选的持久化方式还有磁盘。  
3. transferSaveFile参数的默认值buffer.log.0所在路径是系统安装目录下flows子目录中对应流程实例目录下的share子目录，目标数据文件仅按尺寸实现滚动记录，当尺寸超过设定的阈值将按数字递增序列创建新的目标数据文件流，从而完成文件切换操作。  
