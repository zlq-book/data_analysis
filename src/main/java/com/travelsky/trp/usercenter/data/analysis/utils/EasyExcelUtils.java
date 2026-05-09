package com.travelsky.trp.usercenter.data.analysis.utils;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelReader;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.converters.Converter;
import com.alibaba.excel.converters.ReadConverterContext;
import com.alibaba.excel.converters.WriteConverterContext;
import com.alibaba.excel.enums.CellDataTypeEnum;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.excel.metadata.data.ReadCellData;
import com.alibaba.excel.metadata.data.WriteCellData;
import com.alibaba.excel.read.metadata.ReadSheet;
import com.alibaba.excel.read.metadata.holder.csv.CsvReadWorkbookHolder;
import com.alibaba.excel.support.ExcelTypeEnum;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.alibaba.excel.write.metadata.style.WriteCellStyle;
import com.alibaba.excel.write.metadata.style.WriteFont;
import com.alibaba.excel.write.style.HorizontalCellStyleStrategy;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * EasyExcel 4.0.x 工具类
 * 提供Excel读取、写入的常用功能，支持xlsx、xls、csv等多种格式
 */
public class EasyExcelUtils {

    private static final Logger logger = LoggerFactory.getLogger(EasyExcelUtils.class);

    private final static Charset UTF8 = StandardCharsets.UTF_8;
    private final static Charset GBK = Charset.forName("GBK");

    /**
     * 自定义BigDecimal转换器，用于处理空值情况
     * 当CSV中包含空字符串时，将其转换为null而不是抛出异常
     */
    public static class SafeBigDecimalConverter implements Converter<BigDecimal> {
        
        @Override
        public Class<?> supportJavaTypeKey() {
            return BigDecimal.class;
        }
        
        @Override
        public CellDataTypeEnum supportExcelTypeKey() {
            return CellDataTypeEnum.STRING;
        }
        
        @Override
        public BigDecimal convertToJavaData(ReadConverterContext<?> context) {
            ReadCellData<?> cellData = context.getReadCellData();
            if (cellData == null) {
                return null;
            }
            
            String stringValue = cellData.getStringValue();
            if (stringValue == null || stringValue.trim().isEmpty()) {
                return null;
            }
            
            // 清理单引号前缀
            stringValue = cleanExcelFormatPrefix(stringValue);
            
            try {
                return new BigDecimal(stringValue.trim());
            } catch (NumberFormatException e) {
                logger.warn("无法将值 '{}' 转换为BigDecimal，返回null", stringValue);
                return null;
            }
        }
        
        @Override
        public WriteCellData<?> convertToExcelData(WriteConverterContext<BigDecimal> context) {
            BigDecimal value = context.getValue();
            if (value == null) {
                return new WriteCellData<>("");
            }
            return new WriteCellData<>(value.toString());
        }
    }
    
    /**
     * 通用字符串清理转换器
     * 自动清理Excel/CSV中常见的格式前缀（如单引号 ')
     */
    public static class CleanStringConverter implements Converter<String> {
        
        @Override
        public Class<?> supportJavaTypeKey() {
            return String.class;
        }
        
        @Override
        public CellDataTypeEnum supportExcelTypeKey() {
            return CellDataTypeEnum.STRING;
        }
        
        @Override
        public String convertToJavaData(ReadConverterContext<?> context) {
            ReadCellData<?> cellData = context.getReadCellData();
            if (cellData == null) {
                return null;
            }
            
            String stringValue = cellData.getStringValue();
            if (stringValue == null) {
                return null;
            }
            
            // 清理单引号前缀和首尾空格
            return cleanExcelFormatPrefix(stringValue);
        }
        
        @Override
        public WriteCellData<?> convertToExcelData(WriteConverterContext<String> context) {
            String value = context.getValue();
            if (value == null) {
                return new WriteCellData<>("");
            }
            return new WriteCellData<>(value);
        }
    }
    
    /**
     * 整数类型清理转换器 (Integer)
     * 处理带有单引号前缀的整数
     */
    public static class CleanIntegerConverter implements Converter<Integer> {
        
        @Override
        public Class<?> supportJavaTypeKey() {
            return Integer.class;
        }
        
        @Override
        public CellDataTypeEnum supportExcelTypeKey() {
            return CellDataTypeEnum.STRING;
        }
        
        @Override
        public Integer convertToJavaData(ReadConverterContext<?> context) {
            ReadCellData<?> cellData = context.getReadCellData();
            if (cellData == null) {
                return null;
            }
            
            String stringValue = cellData.getStringValue();
            if (stringValue == null || stringValue.trim().isEmpty()) {
                return null;
            }
            
            // 清理单引号前缀
            stringValue = cleanExcelFormatPrefix(stringValue);
            
            try {
                return Integer.parseInt(stringValue.trim());
            } catch (NumberFormatException e) {
                logger.warn("无法将值 '{}' 转换为Integer，返回null", stringValue);
                return null;
            }
        }
        
        @Override
        public WriteCellData<?> convertToExcelData(WriteConverterContext<Integer> context) {
            Integer value = context.getValue();
            if (value == null) {
                return new WriteCellData<>("");
            }
            return new WriteCellData<>(value.toString());
        }
    }
    
    /**
     * 长整数类型清理转换器 (Long)
     * 处理带有单引号前缀的长整数
     */
    public static class CleanLongConverter implements Converter<Long> {
        
        @Override
        public Class<?> supportJavaTypeKey() {
            return Long.class;
        }
        
        @Override
        public CellDataTypeEnum supportExcelTypeKey() {
            return CellDataTypeEnum.STRING;
        }
        
        @Override
        public Long convertToJavaData(ReadConverterContext<?> context) {
            ReadCellData<?> cellData = context.getReadCellData();
            if (cellData == null) {
                return null;
            }
            
            String stringValue = cellData.getStringValue();
            if (stringValue == null || stringValue.trim().isEmpty()) {
                return null;
            }
            
            // 清理单引号前缀
            stringValue = cleanExcelFormatPrefix(stringValue);
            
            try {
                return Long.parseLong(stringValue.trim());
            } catch (NumberFormatException e) {
                logger.warn("无法将值 '{}' 转换为Long，返回null", stringValue);
                return null;
            }
        }
        
        @Override
        public WriteCellData<?> convertToExcelData(WriteConverterContext<Long> context) {
            Long value = context.getValue();
            if (value == null) {
                return new WriteCellData<>("");
            }
            return new WriteCellData<>(value.toString());
        }
    }
    
    /**
     * 浮点数类型清理转换器 (Double)
     * 处理带有单引号前缀的浮点数
     */
    public static class CleanDoubleConverter implements Converter<Double> {
        
        @Override
        public Class<?> supportJavaTypeKey() {
            return Double.class;
        }
        
        @Override
        public CellDataTypeEnum supportExcelTypeKey() {
            return CellDataTypeEnum.STRING;
        }
        
        @Override
        public Double convertToJavaData(ReadConverterContext<?> context) {
            ReadCellData<?> cellData = context.getReadCellData();
            if (cellData == null) {
                return null;
            }
            
            String stringValue = cellData.getStringValue();
            if (stringValue == null || stringValue.trim().isEmpty()) {
                return null;
            }
            
            // 清理单引号前缀
            stringValue = cleanExcelFormatPrefix(stringValue);
            
            try {
                return Double.parseDouble(stringValue.trim());
            } catch (NumberFormatException e) {
                logger.warn("无法将值 '{}' 转换为Double，返回null", stringValue);
                return null;
            }
        }
        
        @Override
        public WriteCellData<?> convertToExcelData(WriteConverterContext<Double> context) {
            Double value = context.getValue();
            if (value == null) {
                return new WriteCellData<>("");
            }
            return new WriteCellData<>(value.toString());
        }
    }
    
    /**
     * 清理Excel/CSV常见的格式前缀
     * - 单引号 ' ：Excel用于强制文本格式
     * - 等号 = ：Excel公式前缀
     * - 双引号 " ：CSV字段包裹
     * - 首尾空格
     * 
     * @param value 原始字符串
     * @return 清理后的字符串
     */
    private static String cleanExcelFormatPrefix(String value) {
        if (value == null) {
            return null;
        }
        
        String cleaned = value.trim();
        
        // 移除单引号前缀 (Excel强制文本格式)
        if (cleaned.startsWith("'")) {
            cleaned = cleaned.substring(1);
        }
        
        // 移除等号前缀 (Excel公式)
        if (cleaned.startsWith("=")) {
            cleaned = cleaned.substring(1);
        }
        
        // 移除首尾双引号 (CSV字段包裹)
        if (cleaned.startsWith("\"") && cleaned.endsWith("\"") && cleaned.length() > 1) {
            cleaned = cleaned.substring(1, cleaned.length() - 1);
        }
        
        return cleaned.trim();
    }

    /**
     * 简单读取Excel文件
     * @param filePath Excel文件路径
     * @param clazz 数据实体类
     * @param <T> 泛型类型
     * @return 读取的数据列表
     */
    public static <T> List<T> readExcel(String filePath, Class<T> clazz) {
        List<T> dataList = new ArrayList<>();
        try {
            EasyExcel.read(filePath, clazz, new AnalysisEventListener<T>() {
                @Override
                public void invoke(T data, AnalysisContext context) {
                    dataList.add(data);
                }

                @Override
                public void doAfterAllAnalysed(AnalysisContext context) {
                    logger.info("Excel读取完成，共读取{}条数据", dataList.size());
                }
            }).sheet().doRead();
        } catch (Exception e) {
            logger.error("读取Excel文件异常: {}", e.getMessage(), e);
        }
        return dataList;
    }

    /**
     * 读取Excel文件（可指定表头行数，1-based；例如表头在第4行，传 headRowNumber=4）
     * @param filePath Excel文件路径
     * @param clazz 数据实体类
     * @param headRowNumber 表头行数（1-based）。如果表头位于第N行且占1行，请传N
     * @param <T> 泛型类型
     * @return 读取的数据列表
     */
    public static <T> List<T> readExcelWithHeadRow(String filePath, Class<T> clazz, int headRowNumber) {
        List<T> dataList = new ArrayList<>();
        try {
            EasyExcel.read(filePath, clazz, new AnalysisEventListener<T>() {
                @Override
                public void invoke(T data, AnalysisContext context) {
                    dataList.add(data);
                }

                @Override
                public void doAfterAllAnalysed(AnalysisContext context) {
                    logger.info("Excel读取完成，共读取{}条数据", dataList.size());
                }
            })
            .headRowNumber(headRowNumber).sheet().doRead();
        } catch (Exception e) {
            logger.error("读取Excel文件异常: {}", e.getMessage(), e);
        }
        return dataList;
    }

    /**
     * 读取Excel文件（带数据处理回调）
     * @param filePath Excel文件路径
     * @param clazz 数据实体类
     * @param dataConsumer 数据处理回调函数
     * @param <T> 泛型类型
     */
    public static <T> void readExcel(String filePath, Class<T> clazz, Consumer<T> dataConsumer) {
        try {
            EasyExcel.read(filePath, clazz, new AnalysisEventListener<T>() {
                @Override
                public void invoke(T data, AnalysisContext context) {
                    dataConsumer.accept(data);
                }

                @Override
                public void doAfterAllAnalysed(AnalysisContext context) {
                    logger.info("Excel读取完成");
                }
            }).sheet().doRead();
        } catch (Exception e) {
            logger.error("读取Excel文件异常: {}", e.getMessage(), e);
        }
    }

    /**
     * 读取Excel文件（指定sheet）
     * @param filePath Excel文件路径
     * @param clazz 数据实体类
     * @param sheetNo sheet索引（从0开始）
     * @param <T> 泛型类型
     * @return 读取的数据列表
     */
    public static <T> List<T> readExcel(String filePath, Class<T> clazz, int sheetNo) {
        List<T> dataList = new ArrayList<>();
        try {
            EasyExcel.read(filePath, clazz, new AnalysisEventListener<T>() {
                @Override
                public void invoke(T data, AnalysisContext context) {
                    dataList.add(data);
                }

                @Override
                public void doAfterAllAnalysed(AnalysisContext context) {
                    logger.info("Excel读取完成，共读取{}条数据", dataList.size());
                }
            }).sheet(sheetNo).doRead();
        } catch (Exception e) {
            logger.error("读取Excel文件异常: {}", e.getMessage(), e);
        }
        return dataList;
    }


    /**
     * 从InputStream读取CSV（自定义分隔符）
     * 使用EasyExcel 3.0.5标准API实现，支持BigDecimal空值处理
     *
     * @param <T>           泛型类型
     * @param inputStream   输入流
     * @param clazz         数据实体类
     * @param delimiter     自定义分隔符
     * @param headRowNumber
     * @return 读取的数据列表
     */
    public static <T> List<T> readCsvFromStream(InputStream inputStream, Class<T> clazz, char delimiter, Integer headRowNumber,Charset charset) {
        return readCsvFromStream(inputStream, clazz, delimiter, headRowNumber, charset, null);
    }

    /**
     * 从InputStream读取CSV（自定义分隔符，支持后处理函数）
     * 
     * @param <T>            泛型类型
     * @param inputStream    输入流
     * @param clazz          数据实体类
     * @param delimiter      自定义分隔符
     * @param headRowNumber  表头行号
     * @param charset        字符集
     * @param postProcessor  后处理函数，接收数据对象和上下文进行处理
     * @return 读取的数据列表
     */
    public static <T> List<T> readCsvFromStream(InputStream inputStream, Class<T> clazz, char delimiter, Integer headRowNumber, Charset charset, BiConsumer<T, AnalysisContext> postProcessor) {
        List<T> dataList = new ArrayList<>();
        if (headRowNumber == null) {
            headRowNumber = 0;
        }
        ExcelReader excelReader = null;
        try {
            excelReader = EasyExcel.read(inputStream, clazz, new AnalysisEventListener<T>() {
                @Override
                public void invoke(T data, AnalysisContext context) {
                    if (postProcessor != null) {
                        postProcessor.accept(data, context);
                    }
                    dataList.add(data);
                }

                @Override
                public void doAfterAllAnalysed(AnalysisContext context) {
                    logger.info("CSV读取完成，共读取{}条数据", dataList.size());
                }
            }).headRowNumber(headRowNumber)
            .registerConverter(new SafeBigDecimalConverter())
            .registerConverter(new CleanStringConverter())
            .registerConverter(new CleanIntegerConverter())
            .registerConverter(new CleanLongConverter())
            .registerConverter(new CleanDoubleConverter())
            .excelType(ExcelTypeEnum.CSV).charset(charset)
            .build();
            
            if (excelReader.analysisContext().readWorkbookHolder() instanceof CsvReadWorkbookHolder) {
                CsvReadWorkbookHolder csvReadWorkbookHolder =
                        (CsvReadWorkbookHolder) excelReader.analysisContext().readWorkbookHolder();
                csvReadWorkbookHolder.setCsvFormat(
                        csvReadWorkbookHolder.getCsvFormat().withDelimiter(delimiter)
                );
            }

            ReadSheet readSheet = EasyExcel.readSheet(0).build();
            excelReader.read(readSheet);
        } catch (Exception e) {
            logger.error("读取CSV文件异常: {}", e.getMessage(), e);
        } finally {
            if (excelReader != null) {
                try {
                    excelReader.finish();
                } catch (Exception ignore) {
                }
            }
        }

        return dataList;
    }

    public static <T> List<T> readCsvFromStreamUTF8(InputStream inputStream, Class<T> clazz, char delimiter, Integer headRowNumber) {
       return readCsvFromStream(inputStream,clazz,delimiter,headRowNumber,UTF8);
    }

    public static <T> List<T> readCsvFromStreamUTF8(InputStream inputStream, Class<T> clazz, char delimiter, Integer headRowNumber, BiConsumer<T, AnalysisContext> postProcessor) {
       return readCsvFromStream(inputStream,clazz,delimiter,headRowNumber,UTF8, postProcessor);
    }

    public static <T> List<T> readCsvFromStreamGBK(InputStream inputStream, Class<T> clazz, char delimiter, Integer headRowNumber) {
        return readCsvFromStream(inputStream,clazz,delimiter,headRowNumber,GBK);
    }

    public static <T> List<T> readCsvFromStreamGBK(InputStream inputStream, Class<T> clazz, char delimiter, Integer headRowNumber, BiConsumer<T, AnalysisContext> postProcessor) {
        return readCsvFromStream(inputStream,clazz,delimiter,headRowNumber,GBK, postProcessor);
    }
    /**
     * 读取Excel所有sheet数据
     * @param filePath Excel文件路径
     * @param clazz 数据实体类
     * @param <T> 泛型类型
     * @return 每个sheet的数据列表
     */
    public static <T> List<List<T>> readAllSheets(String filePath, Class<T> clazz) {
        List<List<T>> allSheetsData = new ArrayList<>();
        ExcelReader excelReader = null;
        try {
            excelReader = EasyExcel.read(filePath, clazz, null).build();
            // 获取所有sheet信息
            List<ReadSheet> readSheets = excelReader.excelExecutor().sheetList();

            for (ReadSheet readSheet : readSheets) {
                List<T> sheetData = new ArrayList<>();
                excelReader.read(EasyExcel.readSheet(readSheet.getSheetNo())
                    .registerReadListener(new AnalysisEventListener<T>() {
                        @Override
                        public void invoke(T data, AnalysisContext context) {
                            sheetData.add(data);
                        }

                        @Override
                        public void doAfterAllAnalysed(AnalysisContext context) {
                            logger.info("Sheet[{}]读取完成，共{}条数据",
                                readSheet.getSheetName(), sheetData.size());
                        }
                    }).build());
                allSheetsData.add(sheetData);
            }
        } catch (Exception e) {
            logger.error("读取Excel所有sheet异常: {}", e.getMessage(), e);
        } finally {
            if (excelReader != null) {
                try {
                    excelReader.finish();
                } catch (Exception ignore) {
                }
            }
        }
        return allSheetsData;
    }

    /**
     * 简单写入Excel文件
     * @param filePath 输出文件路径
     * @param data 数据列表
     * @param clazz 数据实体类
     * @param <T> 泛型类型
     */
    public static <T> void writeExcel(String filePath, List<T> data, Class<T> clazz) {
        try {
            EasyExcel.write(filePath, clazz)
                .registerWriteHandler(getDefaultCellStyle())
                .sheet("Sheet1")
                .doWrite(data);
            logger.info("Excel写入完成: {}, 共{}条数据", filePath, data.size());
        } catch (Exception e) {
            logger.error("写入Excel文件异常: {}", e.getMessage(), e);
        }
    }

    /**
     * 写入Excel文件（指定sheet名称）
     * @param filePath 输出文件路径
     * @param data 数据列表
     * @param clazz 数据实体类
     * @param sheetName sheet名称
     * @param <T> 泛型类型
     */
    public static <T> void writeExcel(String filePath, List<T> data, Class<T> clazz, String sheetName) {
        try {
            EasyExcel.write(filePath, clazz)
                .registerWriteHandler(getDefaultCellStyle())
                .sheet(sheetName)
                .doWrite(data);
            logger.info("Excel写入完成: {}, Sheet: {}, 共{}条数据", filePath, sheetName, data.size());
        } catch (Exception e) {
            logger.error("写入Excel文件异常: {}", e.getMessage(), e);
        }
    }

    /**
     * 写入Excel到OutputStream
     * @param outputStream 输出流
     * @param data 数据列表
     * @param clazz 数据实体类
     * @param <T> 泛型类型
     */
    public static <T> void writeExcel(OutputStream outputStream, List<T> data, Class<T> clazz) {
        try {
            EasyExcel.write(outputStream, clazz)
                .registerWriteHandler(getDefaultCellStyle())
                .sheet("Sheet1")
                .doWrite(data);
            logger.info("Excel写入完成，共{}条数据", data.size());
        } catch (Exception e) {
            logger.error("写入Excel文件异常: {}", e.getMessage(), e);
        }
    }

    /**
     * 写入多个sheet的Excel文件
     * @param filePath 输出文件路径
     * @param sheetsData 多个sheet的数据和配置
     */
    public static void writeMultiSheetExcel(String filePath, List<SheetWriteData<?>> sheetsData) {
        ExcelWriter excelWriter = null;
        try {
            // 在 3.x 中，推荐在 writer 构建阶段注册样式等全局处理器
            excelWriter = EasyExcel.write(filePath)
                .registerWriteHandler(getDefaultCellStyle())
                .build();

            for (int i = 0; i < sheetsData.size(); i++) {
                SheetWriteData<?> sheetData = sheetsData.get(i);
                WriteSheet writeSheet = EasyExcel.writerSheet(i, sheetData.getSheetName())
                    .head(sheetData.getClazz())
                    .build();
                excelWriter.write(sheetData.getData(), writeSheet);
            }
            logger.info("多Sheet Excel写入完成: {}", filePath);
        } catch (Exception e) {
            logger.error("写入多Sheet Excel文件异常: {}", e.getMessage(), e);
        } finally {
            if (excelWriter != null) {
                try {
                    excelWriter.finish();
                } catch (Exception ignore) {
                }
            }
        }
    }

    /**
     * 读取Excel为Map列表（不需要实体类）
     * @param filePath Excel文件路径
     * @return Map列表，key为列名，value为单元格值
     */
    public static List<Map<Integer, String>> readExcelAsMap(String filePath) {
        List<Map<Integer, String>> dataList = new ArrayList<>();
        try {
            EasyExcel.read(filePath, new AnalysisEventListener<Map<Integer, String>>() {
                @Override
                public void invoke(Map<Integer, String> data, AnalysisContext context) {
                    dataList.add(data);
                }

                @Override
                public void doAfterAllAnalysed(AnalysisContext context) {
                    logger.info("Excel读取完成，共读取{}条数据", dataList.size());
                }
            }).sheet().doRead();
        } catch (Exception e) {
            logger.error("读取Excel文件异常: {}", e.getMessage(), e);
        }
        return dataList;
    }

    /**
     * 从InputStream读取CSV为Map列表（不需要实体类，支持自定义分隔符）
     * 使用EasyExcel 3.0.5标准API实现
     * @param inputStream 输入流
     * @param delimiter 自定义分隔符
     * @return Map列表，key为列索引，value为单元格值
     */
    public static List<Map<Integer, String>> readCsvAsMapFromStream(InputStream inputStream, char delimiter) {
        List<Map<Integer, String>> dataList = new ArrayList<>();
        ExcelReader excelReader = null;
        try {
            excelReader = EasyExcel.read(inputStream, new AnalysisEventListener<Map<Integer, String>>() {
                @Override
                public void invoke(Map<Integer, String> data, AnalysisContext context) {
                    dataList.add(data);
                }

                @Override
                public void doAfterAllAnalysed(AnalysisContext context) {
                    logger.info("CSV读取完成，共读取{}条数据", dataList.size());
                }
            }).build();

            // 获取CSV读取上下文并设置自定义分隔符
            if (excelReader.analysisContext().readWorkbookHolder() instanceof CsvReadWorkbookHolder) {
                CsvReadWorkbookHolder csvReadWorkbookHolder = 
                    (CsvReadWorkbookHolder) excelReader.analysisContext().readWorkbookHolder();
                // 设置自定义分隔符
                csvReadWorkbookHolder.setCsvFormat(
                    csvReadWorkbookHolder.getCsvFormat().withDelimiter(delimiter)
                );
            }

            ReadSheet readSheet = EasyExcel.readSheet(0).build();
            excelReader.read(readSheet);
        } catch (Exception e) {
            logger.error("读取CSV文件异常: {}", e.getMessage(), e);
        } finally {
            if (excelReader != null) {
                try {
                    excelReader.finish();
                } catch (Exception ignore) {
                }
            }
        }
        return dataList;
    }

    /**
     * 读取CSV文件为Map列表（不需要实体类，支持自定义分隔符）
     * @param filePath CSV文件路径
     * @param delimiter 自定义分隔符
     * @return Map列表，key为列索引，value为单元格值
     */
    public static List<Map<Integer, String>> readCsvAsMap(String filePath, char delimiter) {
        try (FileInputStream inputStream = new FileInputStream(filePath)) {
            return readCsvAsMapFromStream(inputStream, delimiter);
        } catch (Exception e) {
            logger.error("读取CSV文件异常: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }

    /**
     * 获取默认的单元格样式
     * @return 单元格样式策略
     */
    private static HorizontalCellStyleStrategy getDefaultCellStyle() {
        // 头的策略
        WriteCellStyle headWriteCellStyle = new WriteCellStyle();
        // 背景色
        headWriteCellStyle.setFillForegroundColor(IndexedColors.PALE_BLUE.getIndex());
        headWriteCellStyle.setFillPatternType(FillPatternType.SOLID_FOREGROUND);
        
        // 字体
        WriteFont headWriteFont = new WriteFont();
        headWriteFont.setFontName("Arial");
        headWriteFont.setFontHeightInPoints((short) 12);
        headWriteFont.setBold(true);
        headWriteCellStyle.setWriteFont(headWriteFont);

        // 内容的策略
        WriteCellStyle contentWriteCellStyle = new WriteCellStyle();
        WriteFont contentWriteFont = new WriteFont();
        contentWriteFont.setFontName("Arial");
        contentWriteFont.setFontHeightInPoints((short) 11);
        contentWriteCellStyle.setWriteFont(contentWriteFont);

        return new HorizontalCellStyleStrategy(headWriteCellStyle, contentWriteCellStyle);
    }

    /**
     * 检查文件是否为Excel文件
     * @param filePath 文件路径
     * @return 是否为Excel文件
     */
    public static boolean isExcelFile(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return false;
        }
        String lowerCasePath = filePath.toLowerCase();
        return lowerCasePath.endsWith(".xlsx") || lowerCasePath.endsWith(".xls");
    }

    /**
     * 检查文件是否存在
     * @param filePath 文件路径
     * @return 文件是否存在
     */
    public static boolean fileExists(String filePath) {
        if (filePath == null || filePath.trim().isEmpty()) {
            return false;
        }
        File file = new File(filePath);
        return file.exists() && file.isFile();
    }

    /**
     * Sheet写入数据封装类
     * @param <T> 数据类型
     */
    public static class SheetWriteData<T> {
        private final String sheetName;
        private final List<T> data;
        private final Class<T> clazz;

        public SheetWriteData(String sheetName, List<T> data, Class<T> clazz) {
            this.sheetName = sheetName;
            this.data = data;
            this.clazz = clazz;
        }

        public String getSheetName() {
            return sheetName;
        }

        public List<T> getData() {
            return data;
        }

        public Class<T> getClazz() {
            return clazz;
        }
    }

    /**
     * CSV常见分隔符常量
     */
    public static class CsvDelimiters {
        public static final char COMMA = ',';           // 逗号（标准CSV）
        public static final char SEMICOLON = ';';       // 分号（欧洲标准）
        public static final char TAB = '\t';            // 制表符
        public static final char PIPE = '|';            // 管道符
        public static final char TILDE = '~';           // 波浪号
        public static final char COLON = ':';           // 冒号
        public static final char SPACE = ' ';           // 空格
        public static final char HASH = '#';            // 井号
        public static final char ARROW_UP = '↑';       // 向上箭头分隔符
    }

    /**
     * 检测CSV文件的可能分隔符
     * @param filePath CSV文件路径
     * @return 检测到的分隔符，如果无法检测则返回null
     */
    public static Character detectCsvDelimiter(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String firstLine = reader.readLine();
            if (firstLine == null) {
                return null;
            }

            // 统计各种可能分隔符的出现频率
            int commaCount = countChar(firstLine, ',');
            int semicolonCount = countChar(firstLine, ';');
            int pipeCount = countChar(firstLine, '|');
            int tabCount = countChar(firstLine, '\t');
            int arrowUpCount = countChar(firstLine, '↑');

            // 返回频率最高的分隔符（排除逗号，因为它可能是数据内容）
            if (arrowUpCount > commaCount && arrowUpCount > semicolonCount &&
                arrowUpCount > pipeCount && arrowUpCount > tabCount) {
                return '↑';
            } else if (semicolonCount > commaCount && semicolonCount > pipeCount &&
                       semicolonCount > tabCount && semicolonCount > arrowUpCount) {
                return ';';
            } else if (pipeCount > commaCount && pipeCount > semicolonCount &&
                       pipeCount > tabCount && pipeCount > arrowUpCount) {
                return '|';
            } else if (tabCount > commaCount && tabCount > semicolonCount &&
                       tabCount > pipeCount && tabCount > arrowUpCount) {
                return '\t';
            } else if (commaCount > 0) {
                return ',';
            }

            return null;
        } catch (Exception e) {
            logger.warn("检测CSV分隔符失败: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 统计字符在字符串中出现的次数
     */
    private static int countChar(String str, char ch) {
        return (int) str.chars().filter(c -> c == ch).count();
    }

    /**
     * 检测CSV文件的字符编码（GBK或UTF-8）
     * 通过读取文件的前几KB字节来判断编码格式
     * 
     * @param inputStream 输入流（支持mark/reset，建议使用BufferedInputStream包装）
     * @return 检测到的编码，如果无法确定则返回UTF-8作为默认值
     */
    public static Charset detectCsvEncoding(InputStream inputStream) {
        if (inputStream == null) {
            logger.warn("输入流为空，返回默认编码UTF-8");
            return UTF8;
        }

        try {
            if (!inputStream.markSupported()) {
                logger.warn("输入流不支持mark/reset，使用BufferedInputStream包装");
                inputStream = new java.io.BufferedInputStream(inputStream);
            }

            inputStream.mark(8192);
            byte[] buffer = new byte[8192];
            int bytesRead = inputStream.read(buffer);
            inputStream.reset();

            if (bytesRead <= 0) {
                logger.warn("无法读取文件内容，返回默认编码UTF-8");
                return UTF8;
            }

            byte[] sample = new byte[bytesRead];
            System.arraycopy(buffer, 0, sample, 0, bytesRead);

            Charset detected = detectEncodingFromBytes(sample);
            logger.info("检测到文件编码: {}", detected.name());
            return detected;

        } catch (Exception e) {
            logger.warn("编码检测失败: {}，返回默认编码UTF-8", e.getMessage());
            return UTF8;
        }
    }

    /**
     * 从字节数组检测编码格式
     * 使用更严格的UTF-8字节序列验证和替换字符检测
     * 
     * @param bytes 文件字节数组（通常为前几KB）
     * @return 检测到的编码
     */
    private static Charset detectEncodingFromBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return UTF8;
        }

        boolean hasUtf8Bom = bytes.length >= 3 && 
            bytes[0] == (byte) 0xEF && 
            bytes[1] == (byte) 0xBB && 
            bytes[2] == (byte) 0xBF;

        if (hasUtf8Bom) {
            logger.info("检测到UTF-8 BOM标记");
            return UTF8;
        }

        boolean isValidUtf8 = isValidUtf8(bytes);
        boolean hasUtf8ReplacementChar = false;
        boolean hasGbkReplacementChar = false;

        try {
            String utf8Str = new String(bytes, UTF8);
            hasUtf8ReplacementChar = utf8Str.indexOf(0xFFFD) >= 0;
        } catch (Exception e) {
            isValidUtf8 = false;
        }

        try {
            String gbkStr = new String(bytes, GBK);
            hasGbkReplacementChar = gbkStr.indexOf(0xFFFD) >= 0;
        } catch (Exception e) {
            hasGbkReplacementChar = true;
        }

        if (isValidUtf8 && !hasUtf8ReplacementChar) {
            if (hasGbkReplacementChar || !isValidGbk(bytes)) {
                logger.info("检测到有效的UTF-8编码（字节序列验证通过且无替换字符）");
                return UTF8;
            }
        }

        if (!hasGbkReplacementChar && isValidGbk(bytes)) {
            if (!isValidUtf8 || hasUtf8ReplacementChar) {
                logger.info("检测到有效的GBK编码（字节序列验证通过且无替换字符）");
                return GBK;
            }
        }

        if (hasUtf8ReplacementChar && !hasGbkReplacementChar) {
            logger.info("UTF-8解码出现替换字符，GBK解码正常，判定为GBK");
            return GBK;
        }

        if (!hasUtf8ReplacementChar && hasGbkReplacementChar) {
            logger.info("UTF-8解码正常，GBK解码出现替换字符，判定为UTF-8");
            return UTF8;
        }

        logger.info("无法明确判断，默认使用UTF-8");
        return UTF8;
    }

    /**
     * 验证字节数组是否符合UTF-8编码规则
     * UTF-8编码规则：
     * - 单字节：0x00-0x7F
     * - 2字节：110xxxxx 10xxxxxx
     * - 3字节：1110xxxx 10xxxxxx 10xxxxxx
     * - 4字节：11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
     * 
     * @param bytes 字节数组
     * @return 是否符合UTF-8编码规则
     */
    private static boolean isValidUtf8(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return true;
        }

        int i = 0;
        while (i < bytes.length) {
            byte b = bytes[i];
            
            if ((b & 0x80) == 0) {
                i++;
            } else if ((b & 0xE0) == 0xC0) {
                if (i + 1 >= bytes.length || (bytes[i + 1] & 0xC0) != 0x80) {
                    return false;
                }
                i += 2;
            } else if ((b & 0xF0) == 0xE0) {
                if (i + 2 >= bytes.length || 
                    (bytes[i + 1] & 0xC0) != 0x80 || 
                    (bytes[i + 2] & 0xC0) != 0x80) {
                    return false;
                }
                i += 3;
            } else if ((b & 0xF8) == 0xF0) {
                if (i + 3 >= bytes.length || 
                    (bytes[i + 1] & 0xC0) != 0x80 || 
                    (bytes[i + 2] & 0xC0) != 0x80 || 
                    (bytes[i + 3] & 0xC0) != 0x80) {
                    return false;
                }
                i += 4;
            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * 验证字节数组是否符合GBK编码规则
     * GBK编码规则：
     * - 单字节：0x00-0x7F（ASCII）
     * - 双字节：0x81-0xFE 0x40-0xFE
     * 
     * @param bytes 字节数组
     * @return 是否符合GBK编码规则
     */
    private static boolean isValidGbk(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return true;
        }

        int i = 0;
        while (i < bytes.length) {
            byte b = bytes[i];
            
            if ((b & 0x80) == 0) {
                i++;
            } else {
                if (i + 1 >= bytes.length) {
                    return false;
                }
                byte b1 = bytes[i];
                byte b2 = bytes[i + 1];
                
                if ((b1 >= (byte) 0x81 && b1 <= (byte) 0xFE) && 
                    (b2 >= (byte) 0x40 && b2 <= (byte) 0xFE)) {
                    i += 2;
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * 解析国航系会员数据Excel文件的5个sheet页
     * @param filePath Excel文件路径
     * @return 包含5个sheet页数据的Map
     */
    public static Map<String, List<?>> readMemberDataExcel(String filePath) {
        Map<String, List<?>> resultMap = new HashMap<>();
        
        // 定义sheet名称与实体类的映射关系
        Map<String, Class<?>> sheetClassMap = new HashMap<>();
        sheetClassMap.put("贵宾会员数量", com.travelsky.dataplatform.module.ods.clk.TOdsClkVipMemberTotal.class);
        sheetClassMap.put("会员升级", com.travelsky.dataplatform.module.ods.clk.TOdsClkMemberUpgrade.class);
        sheetClassMap.put("会员发展", com.travelsky.dataplatform.module.ods.clk.TOdsClkMemberDevelopment.class);
        sheetClassMap.put("里程累积", com.travelsky.dataplatform.module.ods.clk.TOdsClkMileageAccumulation.class);
        sheetClassMap.put("里程消费", com.travelsky.dataplatform.module.ods.clk.TOdsClkMileageConsumption.class);
        
        // 定义sheet名称与结果列表的映射关系
        Map<String, List<Object>> sheetDataMap = new HashMap<>();
        sheetDataMap.put("贵宾会员数量", new ArrayList<>());
        sheetDataMap.put("会员升级", new ArrayList<>());
        sheetDataMap.put("会员发展", new ArrayList<>());
        sheetDataMap.put("里程累积", new ArrayList<>());
        sheetDataMap.put("里程消费", new ArrayList<>());
        
        ExcelReader excelReader = null;
        try {
            excelReader = EasyExcel.read(filePath).build();
            
            // 读取每个sheet
            for (Map.Entry<String, Class<?>> entry : sheetClassMap.entrySet()) {
                String sheetName = entry.getKey();
                Class<?> clazz = entry.getValue();
                List<Object> dataList = sheetDataMap.get(sheetName);
                
                // 读取指定sheet，表头在第2行（headRowNumber=2）
                excelReader.read(EasyExcel.readSheet(sheetName)
                    .headRowNumber(2)
                    .head(clazz)
                    .registerReadListener(new AnalysisEventListener<Object>() {
                        @Override
                        public void invoke(Object data, AnalysisContext context) {
                            // 转换日期字段格式
                            convertDateFields(data);
                            dataList.add(data);
                        }

                        @Override
                        public void doAfterAllAnalysed(AnalysisContext context) {
                            logger.info("Sheet[{}]读取完成，共{}条数据", sheetName, dataList.size());
                        }
                    }).build());
            }
            
            // 将结果放入返回Map
            resultMap.put("贵宾会员数量", sheetDataMap.get("贵宾会员数量"));
            resultMap.put("会员升级", sheetDataMap.get("会员升级"));
            resultMap.put("会员发展", sheetDataMap.get("会员发展"));
            resultMap.put("里程累积", sheetDataMap.get("里程累积"));
            resultMap.put("里程消费", sheetDataMap.get("里程消费"));
            
        } catch (Exception e) {
            logger.error("读取会员数据Excel文件异常: {}", e.getMessage(), e);
        } finally {
            if (excelReader != null) {
                try {
                    excelReader.finish();
                } catch (Exception ignore) {
                }
            }
        }
        
        return resultMap;
    }

    /**
     * 解析FFP达标人群Excel文件的2个sheet页
     * @param filePath Excel文件路径
     * @return 包含2个sheet页数据的Map
     */
    public static Map<String, List<?>> readFFPQualifiedExcel(String filePath) {
        Map<String, List<?>> resultMap = new HashMap<>();
        
        // 存储两个sheet的数据
        List<com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedRegistration> registrationList = new ArrayList<>();
        List<com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedNoRegistration> noRegistrationList = new ArrayList<>();
        
        ExcelReader excelReader = null;
        try {
            excelReader = EasyExcel.read(filePath).build();
            
            // 读取第一个sheet："达标人群（报名）" - 表头在第1行（根据需求）
            excelReader.read(EasyExcel.readSheet("达标人群（报名）")
                .headRowNumber(1)
                .head(com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedRegistration.class)
                .registerReadListener(new AnalysisEventListener<com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedRegistration>() {
                    @Override
                    public void invoke(com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedRegistration data, AnalysisContext context) {
                        registrationList.add(data);
                    }

                    @Override
                    public void doAfterAllAnalysed(AnalysisContext context) {
                        logger.info("Sheet[达标人群（报名）]读取完成，共{}条数据", registrationList.size());
                    }
                }).build());
            
            // 读取第二个sheet："达标人群（无需报名）" - 表头在第1行（根据需求）
            excelReader.read(EasyExcel.readSheet("达标人群（无需报名）")
                .headRowNumber(1)
                .head(com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedNoRegistration.class)
                .registerReadListener(new AnalysisEventListener<com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedNoRegistration>() {
                    @Override
                    public void invoke(com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedNoRegistration data, AnalysisContext context) {
                        noRegistrationList.add(data);
                    }

                    @Override
                    public void doAfterAllAnalysed(AnalysisContext context) {
                        logger.info("Sheet[达标人群（无需报名）]读取完成，共{}条数据", noRegistrationList.size());
                    }
                }).build());
            
            // 将结果放入返回Map
            resultMap.put("达标人群（报名）", registrationList);
            resultMap.put("达标人群（无需报名）", noRegistrationList);
            
        } catch (Exception e) {
            logger.error("读取FFP达标人群Excel文件异常: {}", e.getMessage(), e);
        } finally {
            if (excelReader != null) {
                try {
                    excelReader.finish();
                } catch (Exception ignore) {
                }
            }
        }
        
        return resultMap;
    }

    /**
     * 解析FFP达标人群Excel文件的2个sheet页（简化版）
     * @param filePath Excel文件路径
     * @return 包含2个sheet页数据的Map
     */
    public static Map<String, List<?>> readFFPQualifiedExcelSimple(String filePath) {
        Map<String, List<?>> resultMap = new HashMap<>();
        
        try {
            // 读取第一个sheet：达标人群（报名）
            List<com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedRegistration> registrationList = 
                readExcel(filePath, com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedRegistration.class, 0);
            
            // 读取第二个sheet：达标人群（无需报名）
            List<com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedNoRegistration> noRegistrationList = 
                readExcel(filePath, com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedNoRegistration.class, 1);
            
            // 将结果放入返回Map
            resultMap.put("达标人群（报名）", registrationList);
            resultMap.put("达标人群（无需报名）", noRegistrationList);
            
            logger.info("解析完成：达标人群（报名）{}条，达标人群（无需报名）{}条", 
                registrationList.size(), noRegistrationList.size());
                
        } catch (Exception e) {
            logger.error("读取FFP达标人群Excel文件异常: {}", e.getMessage(), e);
        }
        
        return resultMap;
    }

    /**
     * 从InputStream读取达标人群Excel文件的2个sheet页（用于HDFS流式读取）
     * @param inputStream 输入流
     * @return 包含2个sheet页数据的Map
     */
    public static Map<String, List<?>> readFFPQualifiedExcelFromStream(InputStream inputStream) {
        Map<String, List<?>> resultMap = new HashMap<>();
        
        // 存储两个sheet的数据
        List<com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedRegistration> registrationList = new ArrayList<>();
        List<com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedNoRegistration> noRegistrationList = new ArrayList<>();
        
        ExcelReader excelReader = null;
        try {
            excelReader = EasyExcel.read(inputStream).build();
            
            // 读取第一个sheet："达标人群（报名）" - 表头在第1行
            excelReader.read(EasyExcel.readSheet("达标人群（报名）")
                .headRowNumber(1)
                .head(com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedRegistration.class)
                .registerReadListener(new AnalysisEventListener<com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedRegistration>() {
                    @Override
                    public void invoke(com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedRegistration data, AnalysisContext context) {
                        registrationList.add(data);
                    }

                    @Override
                    public void doAfterAllAnalysed(AnalysisContext context) {
                        logger.info("Sheet[达标人群（报名）]读取完成，共{}条数据", registrationList.size());
                    }
                }).build());
            
            // 读取第二个sheet："达标人群（无需报名）" - 表头在第1行
            excelReader.read(EasyExcel.readSheet("达标人群（无需报名）")
                .headRowNumber(1)
                .head(com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedNoRegistration.class)
                .registerReadListener(new AnalysisEventListener<com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedNoRegistration>() {
                    @Override
                    public void invoke(com.travelsky.dataplatform.module.ods.clk.TOdsClkQualifiedNoRegistration data, AnalysisContext context) {
                        noRegistrationList.add(data);
                    }

                    @Override
                    public void doAfterAllAnalysed(AnalysisContext context) {
                        logger.info("Sheet[达标人群（无需报名）]读取完成，共{}条数据", noRegistrationList.size());
                    }
                }).build());
            
            // 将结果放入返回Map
            resultMap.put("达标人群（报名）", registrationList);
            resultMap.put("达标人群（无需报名）", noRegistrationList);
            
        } catch (Exception e) {
            logger.error("从InputStream读取达标人群Excel文件异常: {}", e.getMessage(), e);
        } finally {
            if (excelReader != null) {
                try {
                    excelReader.finish();
                } catch (Exception ignore) {
                }
            }
        }
        
        return resultMap;
    }

    /**
     * 从InputStream读取Excel（带表头行数和后处理函数）
     * @param <T> 泛型类型
     * @param inputStream 输入流
     * @param clazz 数据实体类
     * @param headRowNumber 表头行数
     * @param postProcessor 后处理函数，接收数据对象和上下文进行处理
     * @return 读取的数据列表
     */
    public static <T> List<T> readExcelWithHeadRowFromStream(InputStream inputStream, Class<T> clazz, int headRowNumber, BiConsumer<T, AnalysisContext> postProcessor) {
        List<T> dataList = new ArrayList<>();
        try {
            EasyExcel.read(inputStream, clazz, new AnalysisEventListener<T>() {
                        @Override
                        public void invoke(T data, AnalysisContext context) {
                            if (postProcessor != null) {
                                postProcessor.accept(data, context);
                            }
                            dataList.add(data);
                        }

                        @Override
                        public void doAfterAllAnalysed(AnalysisContext context) {
                            logger.info("Excel读取完成，共读取{}条数据", dataList.size());
                        }
                    })
                    .registerConverter(new SafeBigDecimalConverter())  // BigDecimal转换器
                    .registerConverter(new CleanStringConverter())     // 字符串清理转换器
                    .registerConverter(new CleanIntegerConverter())    // Integer清理转换器
                    .registerConverter(new CleanLongConverter())       // Long清理转换器
                    .registerConverter(new CleanDoubleConverter())     // Double清理转换器
                    .headRowNumber(headRowNumber).sheet().doRead();
        } catch (Exception e) {
            logger.error("读取Excel文件异常: {}", e.getMessage(), e);
        }
        return dataList;
    }

    /**
     * 从InputStream读取Excel（带表头行数）
     * @param <T> 泛型类型
     * @param inputStream 输入流
     * @param clazz 数据实体类
     * @param headRowNumber 表头行数
     * @return 读取的数据列表
     */
    public static <T> List<T> readExcelWithHeadRowFromStream(InputStream inputStream, Class<T> clazz, int headRowNumber) {
        return readExcelWithHeadRowFromStream(inputStream, clazz, headRowNumber, null);
    }

    /**
     * 从InputStream读取会员数据Excel文件的5个sheet页（用于HDFS流式读取）
     * @param inputStream 输入流
     * @return 包含5个sheet页数据的Map
     */
    public static Map<String, List<?>> readMemberDataExcelFromStream(InputStream inputStream) {
        Map<String, List<?>> resultMap = new HashMap<>();
        
        // 定义sheet名称与实体类的映射关系
        Map<String, Class<?>> sheetClassMap = new HashMap<>();
        sheetClassMap.put("贵宾会员数量", com.travelsky.dataplatform.module.ods.clk.TOdsClkVipMemberTotal.class);
        sheetClassMap.put("会员升级", com.travelsky.dataplatform.module.ods.clk.TOdsClkMemberUpgrade.class);
        sheetClassMap.put("会员发展", com.travelsky.dataplatform.module.ods.clk.TOdsClkMemberDevelopment.class);
        sheetClassMap.put("里程累积", com.travelsky.dataplatform.module.ods.clk.TOdsClkMileageAccumulation.class);
        sheetClassMap.put("里程消费", com.travelsky.dataplatform.module.ods.clk.TOdsClkMileageConsumption.class);
        
        // 定义sheet名称与结果列表的映射关系
        Map<String, List<Object>> sheetDataMap = new HashMap<>();
        sheetDataMap.put("贵宾会员数量", new ArrayList<>());
        sheetDataMap.put("会员升级", new ArrayList<>());
        sheetDataMap.put("会员发展", new ArrayList<>());
        sheetDataMap.put("里程累积", new ArrayList<>());
        sheetDataMap.put("里程消费", new ArrayList<>());
        
        ExcelReader excelReader = null;
        try {
            excelReader = EasyExcel.read(inputStream).build();
            
            // 读取每个sheet
            for (Map.Entry<String, Class<?>> entry : sheetClassMap.entrySet()) {
                String sheetName = entry.getKey();
                Class<?> clazz = entry.getValue();
                List<Object> dataList = sheetDataMap.get(sheetName);
                
                // 读取指定sheet，表头在第2行（headRowNumber=2）
                excelReader.read(EasyExcel.readSheet(sheetName)
                    .headRowNumber(2)
                    .head(clazz)
                    .registerReadListener(new AnalysisEventListener<Object>() {
                        @Override
                        public void invoke(Object data, AnalysisContext context) {
                            
                            dataList.add(data);
                        }

                        @Override
                        public void doAfterAllAnalysed(AnalysisContext context) {
                            logger.info("Sheet[{}]读取完成，共{}条数据", sheetName, dataList.size());
                        }
                    }).build());
            }
            
            // 将结果放入返回Map
            resultMap.put("贵宾会员数量", sheetDataMap.get("贵宾会员数量"));
            resultMap.put("会员升级", sheetDataMap.get("会员升级"));
            resultMap.put("会员发展", sheetDataMap.get("会员发展"));
            resultMap.put("里程累积", sheetDataMap.get("里程累积"));
            resultMap.put("里程消费", sheetDataMap.get("里程消费"));
            
        } catch (Exception e) {
            logger.error("从InputStream读取会员数据Excel文件异常: {}", e.getMessage(), e);
        } finally {
            if (excelReader != null) {
                try {
                    excelReader.finish();
                } catch (Exception ignore) {
                }
            }
        }
        
        return resultMap;
    }

    /**
     * 转换实体对象中的日期字段格式
     * 将Excel中的日期字符串（如"20241027"、"2024-10-27"、"2024/10/27"）转换为标准日期格式"yyyy-MM-dd"
     * 适用于date字段，以便后续存储到Doris数据库的DATE类型字段
     * 
     * @param data 实体对象
     */
    private static void convertDateFields(Object data) {
        if (data == null) {
            return;
        }

        try {
            // 获取所有字段
            java.lang.reflect.Field[] fields = data.getClass().getDeclaredFields();
            
            for (java.lang.reflect.Field field : fields) {
                // 只处理名为"date"的String类型字段
                if ("date".equals(field.getName()) && field.getType() == String.class) {
                    field.setAccessible(true);
                    String dateValue = (String) field.get(data);
                    
                    if (dateValue != null && !dateValue.trim().isEmpty()) {
                        // 转换日期格式
                        String convertedDate = convertDateString(dateValue.trim());
                        field.set(data, convertedDate);
                        logger.debug("日期字段转换: {} -> {}", dateValue, convertedDate);
                    }
                    break; // 找到date字段后退出循环
                }
            }
        } catch (Exception e) {
            logger.warn("转换日期字段失败: {}", e.getMessage());
        }
    }

    /**
     * 将各种日期字符串格式转换为标准格式 "yyyy-MM-dd"
     * 支持的输入格式：
     * - yyyyMMdd (如 20241027)
     * - yyyy-MM-dd (如 2024-10-27)
     * - yyyy/MM/dd (如 2024/10/27)
     * - yyyy.MM.dd (如 2024.10.27)
     * - yyyy年MM月dd日 (如 2024年10月27日)
     * 
     * @param dateStr 输入的日期字符串
     * @return 标准格式的日期字符串 "yyyy-MM-dd"，转换失败则返回原字符串
     */
    private static String convertDateString(String dateStr) {
        if (dateStr == null || dateStr.isEmpty()) {
            return dateStr;
        }

        try {
            // 移除所有空白字符
            dateStr = dateStr.replaceAll("\\s+", "");
            
            // 情况1: yyyyMMdd 格式（8位纯数字）
            if (dateStr.matches("^\\d{8}$")) {
                String year = dateStr.substring(0, 4);
                String month = dateStr.substring(4, 6);
                String day = dateStr.substring(6, 8);
                return year + "-" + month + "-" + day;
            }
            
            // 情况2: yyyy-MM-dd 格式（已经是标准格式）
            if (dateStr.matches("^\\d{4}-\\d{1,2}-\\d{1,2}$")) {
                String[] parts = dateStr.split("-");
                String year = parts[0];
                String month = String.format("%02d", Integer.parseInt(parts[1]));
                String day = String.format("%02d", Integer.parseInt(parts[2]));
                return year + "-" + month + "-" + day;
            }
            
            // 情况3: yyyy/MM/dd 格式
            if (dateStr.matches("^\\d{4}/\\d{1,2}/\\d{1,2}$")) {
                String[] parts = dateStr.split("/");
                String year = parts[0];
                String month = String.format("%02d", Integer.parseInt(parts[1]));
                String day = String.format("%02d", Integer.parseInt(parts[2]));
                return year + "-" + month + "-" + day;
            }
            
            // 情况4: yyyy.MM.dd 格式
            if (dateStr.matches("^\\d{4}\\.\\d{1,2}\\.\\d{1,2}$")) {
                String[] parts = dateStr.split("\\.");
                String year = parts[0];
                String month = String.format("%02d", Integer.parseInt(parts[1]));
                String day = String.format("%02d", Integer.parseInt(parts[2]));
                return year + "-" + month + "-" + day;
            }
            
            // 情况5: yyyy年MM月dd日 格式
            if (dateStr.matches("^\\d{4}年\\d{1,2}月\\d{1,2}日$")) {
                dateStr = dateStr.replace("年", "-").replace("月", "-").replace("日", "");
                String[] parts = dateStr.split("-");
                String year = parts[0];
                String month = String.format("%02d", Integer.parseInt(parts[1]));
                String day = String.format("%02d", Integer.parseInt(parts[2]));
                return year + "-" + month + "-" + day;
            }
            
            // 如果都不匹配，记录警告并返回原字符串
            logger.warn("未识别的日期格式: {}", dateStr);
            return dateStr;
            
        } catch (Exception e) {
            logger.warn("日期格式转换失败: {}, 错误: {}", dateStr, e.getMessage());
            return dateStr;
        }
    }
}