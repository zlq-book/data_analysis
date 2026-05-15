# Data Analysis

> 基于Apache Flink的企业级数据分析和处理平台

[![Java](https://img.shields.io/badge/Java-8%2B-brightgreen.svg)](https://www.oracle.com/java/)
[![Maven](https://img.shields.io/badge/Maven-3.6%2B-blue.svg)](https://maven.apache.org/)
[![Flink](https://img.shields.io/badge/Apache%20Flink-1.16.2-orange.svg)](https://flink.apache.org/)
[![License](https://img.shields.io/badge/License-Open%20Source-brightgreen.svg)](#)

## 📋 项目概述

Data Analysis 是一个基于 **Apache Flink** 的企业级大数据分析和处理平台，为航空旅游等行业提供完整的数据采集、转换、分析和存储解决方案。支持多种数据源集成、多格式文件处理、分布式流处理等核心功能。

### 🎯 主要特性

- **🔗 多数据源支持**：HDFS、Kafka、JDBC、SFTP、Oracle、MySQL 等多源异构数据集成
- **📊 多格式文件处理**：支持 Excel、CSV、Parquet 等多种数据格式自动识别和解析
- **⚡ 分布式流处理**：基于 Flink 的实时流处理和批处理能力
- **🔐 安全可靠**：Kerberos 认证、数据加密、容错恢复机制
- **🌍 环境适配**：支持开发、测试、UAT、生产多环境部署
- **📝 动态配置**：基于 Nacos 的配置中心支持，实现动态配置管理

---

## 🛠 技术栈

| 技术 | 版本 | 说明 |
|------|------|------|
| **Java** | 1.8 | 核心开发语言 |
| **Apache Flink** | 1.16.2 | 流处理框架 |
| **Hadoop** | 3.1.0 | 分布式文件系统 |
| **Kafka** | 2.8.2 | 消息队列 |
| **EasyExcel** | 3.1.0 | Excel 数据处理库 |
| **Nacos** | 2.2.3 | 配置中心 & 服务发现 |
| **Doris** | 24.0.1 | OLAP 数据库（可选） |

---

## 📦 项目结构

```
data_analysis/
├── src/
│   ├── main/
│   │   ├── java/                     # Java 源代码
│   │   │   └── com/travelsky/
│   │   │       └── dataplatform/
│   │   │           ├── main/         # 主程序入口
│   │   │           ├── utils/        # 工具类
│   │   │           └── entities/     # 数据实体
│   │   └── resources/                # 配置文件
│   │       ├── application.properties
│   │       └── application-config.properties
│   └── test/                         # 测试代码
├── file/                             # 文件处理目录
├── lib/                              # 本地依赖库
├── pom.xml                           # Maven 配置文件
└── MULTI_FORMAT_SUPPORT_GUIDE.md    # 多格式支持指南
```

---

## 🚀 快速开始

### 前置条件

- **Java 8+** 已安装
- **Maven 3.6+** 已安装
- **Hadoop 集群** 或 HDFS 服务可用
- **Kafka** 服务可用（如需使用）

### 环境配置

1. **克隆项目**
```bash
git clone https://github.com/zlq-book/data_analysis.git
cd data_analysis
```

2. **配置环境变量**
```bash
# 编辑配置文件
vim src/main/resources/application.properties
```

3. **主要配置项**
```properties
# HDFS 配置
hdfs.namenode.rpc.address=hdfs://localhost:9000
hdfs.user=hadoop

# Kafka 配置
kafka.bootstrap.servers=localhost:9092
kafka.topic=data-analysis-topic

# 数据库配置
db.url=jdbc:mysql://localhost:3306/data_analysis
db.username=root
db.password=your_password

# 认证信息
security.kerberos.enabled=false
```

4. **编译项目**
```bash
mvn clean package -Pdev
```

5. **运行应用**
```bash
java -jar target/SdaData-1.0-SNAPSHOT.jar
```

---

## 📖 使用指南

### 1. 数据导入

#### Excel/CSV 文件导入
```java
// 自动格式检测（推荐）
TransferResult result = TrpExcelDataTransferUtils.transferTrpDataViaSftp(
    sftpConfig,
    "sc_b2c_report_sale",
    TOdsScSaleDetail.class,
    "20241011"
);

// 指定 CSV 格式
TransferResult result = TrpExcelDataTransferUtils.transferTrpDataViaSftp(
    sftpConfig,
    "sc_b2c_report_sale",
    TOdsScSaleDetail.class,
    "20241011",
    ".csv"
);
```

#### SFTP 文件处理
```java
// 配置 SFTP 连接
SftpConfig sftpConfig = new SftpConfig();
sftpConfig.setHost("sftp.example.com");
sftpConfig.setPort(22);
sftpConfig.setUsername("user");
sftpConfig.setPassword("password");
```

### 2. Flink 流处理

```java
// 创建流处理环境
StreamExecutionEnvironment env = 
    StreamExecutionEnvironment.getExecutionEnvironment();

// 从 Kafka 读取数据
DataStream<String> stream = env.addSource(
    new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties)
);

// 执行任务
env.execute("Data Analysis Job");
```

### 3. 多格式支持

项目支持的数据格式及分隔符：

| 格式 | 扩展名 | 分隔符支持 |
|------|--------|----------|
| Excel | .xlsx, .xls | 行列格式 |
| CSV | .csv | `,`, `;`, `\|`, `\t`, `↑` |
| Parquet | .parquet | 列式存储 |

详见 [MULTI_FORMAT_SUPPORT_GUIDE.md](./MULTI_FORMAT_SUPPORT_GUIDE.md)

---

## 🏗 编译和部署

### Maven 编译

**开发环境编译**
```bash
mvn clean package -Pdev
```

**测试环境编译**
```bash
mvn clean package -Ptest
```

**UAT 环境编译**
```bash
mvn clean package -Puat
```

**生产环境编译**
```bash
mvn clean package -Pprod
```

### 生成可执行 Jar

```bash
# 使用 Maven Shade 插件打包所有依赖
mvn clean package
# 输出: target/SdaData-1.0-SNAPSHOT.jar
```

### Docker 部署（可选）

```bash
# 创建 Docker 镜像
docker build -t data-analysis:1.0 .

# 运行容器
docker run -d --name data-analysis \
    -e JAVA_OPTS="-Xmx2g" \
    -v /data/logs:/app/logs \
    data-analysis:1.0
```

---

## 🔧 核心功能模块

### 1. 数据采集模块
- HDFS 数据读取
- Kafka 消息消费
- 数据库 SQL 查询
- SFTP 文件传输

### 2. 数据转换模块
- Excel/CSV 解析器
- 数据类型转换
- 字段映射处理
- 数据验证校验

### 3. 数据处理模块
- 流处理（Streaming）
- 批处理（Batch）
- 窗口聚合
- 状态管理

### 4. 数据存储模块
- 关系型数据库（MySQL/Oracle）
- 列式数据库（Doris）
- 文件系统（HDFS）

---

## 🔐 安全特性

- **Kerberos 认证**：支持大数据集群安全认证
- **数据加密**：支持 SFTP 加密传输和数据库连接加密
- **访问控制**：基于角色的细粒度权限管理
- **审计日志**：完整的操作审计和追踪

---

## 📊 监控和日志

### 日志配置

日志输出位置：`logs/` 目录

```properties
# 日志级别配置
log4j.rootLogger=INFO, file

# 日志文件
log4j.appender.file=org.apache.log4j.RollingFileAppender
log4j.appender.file.File=logs/data-analysis.log
```

### 监控指标

- 处理记录数
- 处理耗时
- 错误率
- 资源使用情况

---

## 🐛 常见问题

### Q1: 如何解决依赖冲突？
**A:** 项目使用 `dependencyManagement` 明确指定版本，特别是 commons-compress 库。若仍有冲突：
```bash
mvn dependency:tree
# 查看依赖树并调整排除规则
```

### Q2: SFTP 连接超时？
**A:** 检查网络连接和防火墙设置：
```bash
# 测试 SFTP 连接
sftp -P 22 user@host
```

### Q3: CSV 文件解析失败？
**A:** 确保：
- 文件编码为 UTF-8
- 分隔符正确（支持 `,` `;` `|` `\t` `↑`）
- 字段和表头匹配

---

## 🤝 贡献指南

1. Fork 本项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

---

## 📝 更新日志

### v1.0.0 (2025-05-15)
- ✅ 多格式文件支持（Excel、CSV）
- ✅ Flink 流处理集成
- ✅ SFTP 数据传输
- ✅ 多环境部署配置
- ✅ Nacos 配置中心支持

详见 [CHANGELOG.md](./CHANGELOG.md)（如有）

---

## 📞 联系方式

- **项目地址**：[https://github.com/zlq-book/data_analysis](https://github.com/zlq-book/data_analysis)
- **问题反馈**：[Issues](https://github.com/zlq-book/data_analysis/issues)
- **讨论区**：[Discussions](https://github.com/zlq-book/data_analysis/discussions)

---

## 📄 许可证

本项目采用开源许可证发布。详见 [LICENSE](./LICENSE)

---

## 🙏 致谢

感谢以下开源项目的支持：
- [Apache Flink](https://flink.apache.org/)
- [Alibaba EasyExcel](https://github.com/alibaba/easyexcel)
- [Apache Hadoop](https://hadoop.apache.org/)
- [Alibaba Nacos](https://nacos.io/)

---

**最后更新**: 2026-05-15 | **维护者**: [@zlq-book](https://github.com/zlq-book)
