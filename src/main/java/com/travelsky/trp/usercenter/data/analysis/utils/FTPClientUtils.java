package com.travelsky.trp.usercenter.data.analysis.utils;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * FTP工具类
 * 基于FtpClient实现的FTP连接和文件操作工具
 */
public class FTPClientUtils {

    private static final Logger logger = LoggerFactory.getLogger(FTPClientUtils.class);
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private FTPClient ftp;
    /**
     * 构造函数
     *
     * @param host     FTP服务器地址
     * @param port     FTP服务器端口
     * @param username 用户名
     * @param password 密码
     */
    public FTPClientUtils(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    /**
     * 构造函数，使用默认端口21
     *
     * @param host     FTP服务器地址
     * @param username 用户名
     * @param password 密码
     */
    public FTPClientUtils(String host, String username, String password) {
        this(host, 21, username, password);
    }

    /**
     * 连接FTP服务器
     *
     * @return 是否连接成功
     */
    public boolean connect() {
        try {
            ftp.connect(host, port);
            ftp.login(username, password);
            // 被动模式，避免防火墙问题
            ftp.enterLocalPassiveMode();
            logger.info("FTP通道连接成功: {}:{}", host, port);
            return true;

        } catch (IOException e) {
            logger.error("FTP连接失败: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * 断开FTP连接
     */
    public void disconnect() {
        try {
            if (ftp != null && ftp.isConnected()) {
                ftp.disconnect();
                logger.info("FTP通道已断开");
            }
        } catch (Exception e) {
            logger.error("断开FTP连接时发生错误: {}", e.getMessage(), e);
        }
    }

    /**
     * 切换工作目录
     *
     * @param dirPath 目录路径
     * @return 是否切换成功
     */
    public boolean changeWorkingDirectory(String dirPath) {
        try {
            ftp.changeWorkingDirectory(dirPath);
            logger.info("切换FTP工作目录成功: {}", dirPath);
            return true;
        } catch (IOException e) {
            logger.error("切换FTP工作目录失败: {}, 错误: {}", dirPath, e.getMessage());
            return false;
        }
    }


    /**
     * 检查文件是否存在
     *
     * @param filePath 文件路径
     * @return 是否存在
     */
    public boolean fileExists(String filePath) {
        try {
            ftp.stat(filePath);
            return true;
        } catch (IOException e) {
            // 文件不存在时会抛出异常
            return false;
        }
    }

    /**
     * 列出当前目录的文件
     *
     * @return 文件列表
     */
    @SuppressWarnings("unchecked")
    public FTPFile[] listFiles() {
        try {
            return ftp.listFiles(".");
        } catch (IOException e) {
            logger.error("列出FTP目录文件失败: {}", e.getMessage());
            return new FTPFile[0];
        }
    }

    /**
     * 列出指定目录的文件
     *
     * @param dirPath 目录路径
     * @return 文件列表
     */
    @SuppressWarnings("unchecked")
    public FTPFile[] listFiles(String dirPath) {
        try {
            return ftp.listFiles(dirPath);
        } catch (IOException e) {
            logger.error("列出FTP目录文件失败: {}, 错误: {}", dirPath, e.getMessage());
            return new FTPFile[0];
        }
    }

    /**
     * 获取文件输入流
     *
     * @param filePath 远程文件路径
     * @return 文件输入流
     */
    public InputStream getFileInputStream(String filePath) {
        try {
            InputStream inputStream = ftp.retrieveFileStream(filePath);
            logger.info("获取FTP文件流成功: {}", filePath);
            return inputStream;
        } catch (IOException e) {
            logger.error("获取FTP文件流失败: {}, 错误: {}", filePath, e.getMessage());
            return null;
        }
    }

    /**
     * 下载文件
     *
     * @param remoteFilePath 远程文件路径
     * @param localFilePath  本地文件路径
     * @return 是否下载成功
     */
    public boolean downloadFile(String remoteFilePath, String localFilePath) {
        try (FileOutputStream fos = new FileOutputStream(localFilePath)) {
            boolean success = ftp.retrieveFile(remoteFilePath, fos);
            if (!success) {
                logger.info("retrieveFile 返回 false，错误代码: " + ftp.getReplyCode());
                return false;
            }
            logger.info("FTP文件下载成功: {} -> {}", remoteFilePath, localFilePath);
            return true;
        } catch (IOException e) {
            logger.error("FTP文件下载失败: {} -> {}, 错误: {}", remoteFilePath, localFilePath, e.getMessage());
            return false;
        }
    }


    /**
     * 检查是否连接
     *
     * @return 是否已连接
     */
    public boolean isConnected() {
        return ftp != null && ftp.isConnected();
    }

    /**
     * 获取FTP通道
     *
     * @return FTP通道
     */
    public FTPClient getftp() {
        return ftp;
    }
}