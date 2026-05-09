# TRP多文件格式支持增强

## 功能概述
为 `TrpExcelDataTransferUtils` 增加了多文件格式支持，能够根据文件后缀自动选择合适的解析器：
- **Excel文件** (.xlsx, .xls) → 使用EasyExcel解析器
- **CSV文件** (.csv) → 使用轻量级CSV解析器

## 新增功能特性

### 🎯 自动格式检测
系统会根据文件后缀自动选择解析方式：

```java
// 文件格式检测逻辑
if (fileName.toLowerCase().endsWith(".csv")) {
    // 使用CSV解析器
    dataList = parseCSVFromInputStream(inputStream, entityClass);
} else if (fileName.toLowerCase().endsWith(".xlsx") || fileName.toLowerCase().endsWith(".xls")) {
    // 使用Excel解析器
    dataList = readExcelWithHeadRowFromStream(inputStream, entityClass, 2);
} else {
    // 未知格式默认使用Excel解析器
    logger.warn("未知文件类型，尝试使用Excel解析器: {}", fileName);
    dataList = readExcelWithHeadRowFromStream(inputStream, entityClass, 2);
}
```

### 📊 CSV解析器特性
- **多分隔符支持**：自动检测和支持常用分隔符
  - 逗号 (`,`) - 标准CSV
  - 分号 (`;`) - 欧洲标准
  - 管道 (`|`) - 常用分隔符
  - 制表符 (`\t`) - TSV格式
  - 箭头 (`↑`) - 特殊分隔符
- **引号转义**：支持字段内容包含分隔符的情况
- **UTF-8编码**：正确处理中文字符
- **容错处理**：解析失败时尝试其他分隔符

### 🔍 智能文件查找
新增的方法支持智能文件查找，按优先级自动寻找可用文件：

```java
// 智能文件查找示例
String fileName = findAvailableFile(sftpUtils, "sc_b2c_report_sale", "20241011", ".xlsx");
// 查找顺序：
// 1. sc_b2c_report_sale_20241011.xlsx
// 2. sc_b2c_report_sale_20241011.xls  
// 3. sc_b2c_report_sale_20241011.csv
```

## 方法签名更新

### 原有方法（保持兼容）
```java
// FTP版本
public static <T> TransferResult transferTrpData(FtpConfig ftpConfig, String filePrefix, Class<T> entityClass)
public static <T> TransferResult transferTrpData(FtpConfig ftpConfig, String filePrefix, Class<T> entityClass, String dateSignature)

// SFTP版本
public static <T> TransferResult transferTrpDataViaSftp(SftpConfig sftpConfig, String filePrefix, Class<T> entityClass)
public static <T> TransferResult transferTrpDataViaSftp(SftpConfig sftpConfig, String filePrefix, Class<T> entityClass, String dateSignature)
```

### 新增方法（支持文件格式指定）
```java
// SFTP版本 - 支持文件格式指定
public static <T> TransferResult transferTrpDataViaSftp(SftpConfig sftpConfig, String filePrefix, Class<T> entityClass, String dateSignature, String fileExtension)
```

## 使用示例

### 示例1：自动格式检测（推荐）
```java
// 系统会自动检测文件格式并选择合适的解析器
TrpExcelDataTransferUtils.TransferResult result = TrpExcelDataTransferUtils.transferTrpDataViaSftp(
    sftpConfig,
    "sc_b2c_report_sale",
    TOdsScSaleDetail.class,
    "20241011"
);
```

### 示例2：指定CSV格式
```java
// 明确指定要处理CSV文件
TrpExcelDataTransferUtils.TransferResult result = TrpExcelDataTransferUtils.transferTrpDataViaSftp(
    sftpConfig,
    "sc_b2c_report_sale",
    TOdsScSaleDetail.class,
    "20241011",
    ".csv"  // 指定CSV格式
);
```

### 示例3：指定Excel格式
```java
// 明确指定要处理Excel文件
TrpExcelDataTransferUtils.TransferResult result = TrpExcelDataTransferUtils.transferTrpDataViaSftp(
    sftpConfig,
    "sc_b2c_report_sale",
    TOdsScSaleDetail.class,
    "20241011",
    ".xlsx"  // 指定Excel格式
);
```

## CSV格式要求

### 文件结构
```csv
产品编号,销售金额,销售日期,客户姓名
PRD001,1500.00,2024-10-11,张三
PRD002,2300.50,2024-10-11,李四
```

### 支持的分隔符格式
```csv
# 逗号分隔（标准CSV）
字段1,字段2,字段3

# 分号分隔（欧洲标准）
字段1;字段2;字段3

# 管道分隔
字段1|字段2|字段3

# 制表符分隔（TSV）
字段1	字段2	字段3

# 箭头分隔（特殊格式）
字段1↑字段2↑字段3
```

### 引号转义支持
```csv
"包含,逗号的字段","正常字段","包含""双引号""的字段"
```

## 实体类配置

确保实体类使用 `@ExcelProperty` 注解正确映射字段：

```java
public class TOdsScSaleDetail {
    @ExcelProperty("产品编号")
    private String productCode;
    
    @ExcelProperty("销售金额")
    private BigDecimal saleAmount;
    
    @ExcelProperty("销售日期")
    private LocalDate saleDate;
    
    // ... 其他字段和方法
}
```

## 日志输出示例

### Excel文件处理
```
INFO  检测到Excel文件，使用Excel解析器: sc_b2c_report_sale_20241011.xlsx
INFO  从SFTP文件 sc_b2c_report_sale_20241011.xlsx 解析出 1205 条记录
INFO  TRP SFTP数据转储完成: 文件=sc_b2c_report_sale_20241011.xlsx, 处理记录数=1205
```

### CSV文件处理
```
INFO  检测到CSV文件，使用CSV解析器: sc_b2c_report_sale_20241011.csv
INFO  成功使用分隔符 ',' 解析CSV文件
INFO  从SFTP文件 sc_b2c_report_sale_20241011.csv 解析出 1205 条记录
INFO  TRP SFTP数据转储完成: 文件=sc_b2c_report_sale_20241011.csv, 处理记录数=1205
```

### 智能文件查找
```
INFO  找到文件: sc_b2c_report_sale_20241011.csv (格式: .csv)
WARN  使用逗号分隔符解析CSV失败，尝试其他分隔符
INFO  成功使用分隔符 ';' 解析CSV文件
```

## 错误处理

### 文件不存在
```
ERROR 未找到任何匹配的文件: sc_b2c_report_sale_20241011.{xlsx,xls,csv}
```

### 解析失败
```
ERROR 解析CSV文件异常: 无法识别文件格式或分隔符
WARN  未知文件类型，尝试使用Excel解析器: unknown_file.dat
```

## 性能对比

| 文件格式 | 解析器 | 内存占用 | 解析速度 | 兼容性 |
|----------|--------|----------|----------|--------|
| .xlsx | EasyExcel | 中等 | 快 | 极佳 |
| .xls | EasyExcel | 中等 | 中等 | 极佳 |
| .csv | 轻量CSV | 低 | 极快 | 良好 |

## 最佳实践

1. **优先使用Excel格式**：对于复杂数据结构，Excel格式提供更好的兼容性
2. **CSV适用场景**：简单数据结构、大数据量、跨系统交换
3. **文件命名规范**：保持 `{prefix}_{YYYYMMDD}.{extension}` 格式
4. **字段映射**：确保实体类字段与文件表头完全匹配
5. **错误监控**：关注日志中的格式检测和解析警告信息

## 向后兼容性

- ✅ **完全兼容**：现有代码无需修改
- ✅ **默认行为**：未指定格式时默认查找Excel文件
- ✅ **渐进升级**：可以逐步迁移到新的多格式支持方法

**更新时间**：2025-10-11  
**功能状态**：已完成并测试通过 ✅