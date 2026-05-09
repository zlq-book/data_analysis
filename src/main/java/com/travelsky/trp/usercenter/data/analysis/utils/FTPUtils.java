package com.travelsky.trp.usercenter.data.analysis.utils;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * FTP工具类
 * 提供FTP连接、上传、下载、删除等基础操作
 */
public class FTPUtils {

    private static final Logger logger = LoggerFactory.getLogger(FTPUtils.class);
    
    private final FTPClient ftpClient;
    private final String host;
    private final String port;
    private final String username;
    private final String password;
    private String proxyHost;
    private int proxyPort;
    private boolean useProxy = false;
    
    /**
     * 构造函数
     * @param host FTP服务器地址
     * @param port FTP服务器端口
     * @param username 用户名
     * @param password 密码
     */
    public FTPUtils(String host, String port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
        this.ftpClient = new FTPClient();
    }
    
    /**
     * 构造函数，使用默认端口21
     * @param host FTP服务器地址
     * @param username 用户名
     * @param password 密码
     */
    public FTPUtils(String host, String username, String password) {
        this(host, "21", username, password);
    }
    
    /**
     * 设置SOCKS5代理
     * @param proxyHost 代理服务器地址
     * @param proxyPort 代理服务器端口
     */
    public void setProxy(String proxyHost, int proxyPort) {
        this.proxyHost = proxyHost;
        this.proxyPort = proxyPort;
        this.useProxy = true;
        
        // 设置自定义SocketFactory以支持SOCKS5代理
        if (useProxy) {
            ftpClient.setSocketFactory(new Socks5SocketFactory(proxyHost, proxyPort));
        }
        
        logger.info("已配置SOCKS5代理: {}:{}", proxyHost, proxyPort);
    }
    
    /**
     * 连接并登录FTP服务器
     * @return 是否连接成功
     */
    public boolean connect() {
        try {
            // 直接连接到FTP服务器（如果设置了代理，会通过SocketFactory使用SOCKS5）
            ftpClient.connect(host, Integer.parseInt(port));
            
            // 检查连接响应
            int replyCode = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(replyCode)) {
                logger.error("FTP服务器拒绝连接, 响应码: {}", replyCode);
                return false;
            }
            
            // 登录
            boolean loginSuccess = ftpClient.login(username, password);
            if (!loginSuccess) {
                logger.error("FTP登录失败，用户名或密码错误");
                return false;
            }
            
            // 设置文件传输模式为二进制
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            // 设置为被动模式
            ftpClient.enterLocalPassiveMode();
            
            if (useProxy) {
                logger.info("FTP连接成功: {}:{} (通过SOCKS5代理 {}:{})", host, port, proxyHost, proxyPort);
            } else {
                logger.info("FTP连接成功: {}:{}", host, port);
            }
            return true;
            
        } catch (IOException e) {
            logger.error("FTP连接异常: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 断开FTP连接
     */
    public void disconnect() {
        if (ftpClient != null && ftpClient.isConnected()) {
            try {
                ftpClient.logout();
                ftpClient.disconnect();
                logger.info("FTP连接已断开");
            } catch (IOException e) {
                logger.error("断开FTP连接时发生错误: {}", e.getMessage(), e);
            }
        }
    }
    
    /**
     * 上传文件
     * @param localFilePath 本地文件路径
     * @param remoteFilePath 远程文件路径
     * @return 是否上传成功
     */
    public boolean uploadFile(String localFilePath, String remoteFilePath) {
        try (FileInputStream fis = new FileInputStream(localFilePath)) {
            boolean success = ftpClient.storeFile(remoteFilePath, fis);
            if (success) {
                logger.info("文件上传成功: {} -> {}", localFilePath, remoteFilePath);
            } else {
                logger.error("文件上传失败: {} -> {}", localFilePath, remoteFilePath);
            }
            return success;
        } catch (IOException e) {
            logger.error("上传文件异常: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 下载文件
     * @param remoteFilePath 远程文件路径
     * @param localFilePath 本地文件路径
     * @return 是否下载成功
     */
    public boolean downloadFile(String remoteFilePath, String localFilePath) {
        if (!isConnected()) {
            connect();
        }
        try (FileOutputStream fos = new FileOutputStream(new File(localFilePath))) {
            boolean success = ftpClient.retrieveFile(remoteFilePath, fos);
            if (success) {
                logger.info("文件下载成功: {} -> {}", remoteFilePath, localFilePath);
            } else {
                logger.error("文件下载失败: {} -> {}", remoteFilePath, localFilePath);
            }
            return success;
        } catch (IOException e) {
            logger.error("下载文件异常: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 删除文件
     * @param remoteFilePath 远程文件路径
     * @return 是否删除成功
     */
    public boolean deleteFile(String remoteFilePath) {
        try {
            boolean success = ftpClient.deleteFile(remoteFilePath);
            if (success) {
                logger.info("文件删除成功: {}", remoteFilePath);
            } else {
                logger.error("文件删除失败: {}", remoteFilePath);
            }
            return success;
        } catch (IOException e) {
            logger.error("删除文件异常: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 创建目录
     * @param dirPath 目录路径
     * @return 是否创建成功
     */
    public boolean createDirectory(String dirPath) {
        try {
            boolean success = ftpClient.makeDirectory(dirPath);
            if (success) {
                logger.info("目录创建成功: {}", dirPath);
            } else {
                logger.error("目录创建失败: {}", dirPath);
            }
            return success;
        } catch (IOException e) {
            logger.error("创建目录异常: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 删除目录
     * @param dirPath 目录路径
     * @return 是否删除成功
     */
    public boolean removeDirectory(String dirPath) {
        try {
            boolean success = ftpClient.removeDirectory(dirPath);
            if (success) {
                logger.info("目录删除成功: {}", dirPath);
            } else {
                logger.error("目录删除失败: {}", dirPath);
            }
            return success;
        } catch (IOException e) {
            logger.error("删除目录异常: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 列出指定目录下的文件
     * @param dirPath 目录路径
     * @return 文件列表
     */
    public List<FTPFile> listFiles(String dirPath) {
        List<FTPFile> fileList = new ArrayList<>();
        try {
            FTPFile[] files = ftpClient.listFiles(dirPath);
            Collections.addAll(fileList, files);
            logger.info("列出目录文件成功: {}, 共{}个文件", dirPath, fileList.size());
        } catch (IOException e) {
            logger.error("列出目录文件异常: {}", e.getMessage(), e);
        }
        return fileList;
    }
    
    /**
     * 改变工作目录
     * @param dirPath 目录路径
     * @return 是否切换成功
     */
    public boolean changeWorkingDirectory(String dirPath) {
        try {
            boolean success = ftpClient.changeWorkingDirectory(dirPath);
            if (success) {
                logger.info("切换工作目录成功: {}", dirPath);
            } else {
                logger.error("切换工作目录失败: {}", dirPath);
            }
            return success;
        } catch (IOException e) {
            logger.error("切换工作目录异常: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 获取当前工作目录
     * @return 当前工作目录路径
     */
    public String getCurrentWorkingDirectory() {
        try {
            String currentDir = ftpClient.printWorkingDirectory();
            logger.info("当前工作目录: {}", currentDir);
            return currentDir;
        } catch (IOException e) {
            logger.error("获取当前工作目录异常: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * 检查文件是否存在
     * @param filePath 文件路径
     * @return 是否存在
     */
    public boolean fileExists(String filePath) {
        if (!isConnected()) {
            connect();
        }
        try {
            FTPFile[] files = ftpClient.listFiles(filePath);
            return files.length > 0;
        } catch (IOException e) {
            logger.error("检查文件是否存在异常: {}", e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * 获取FTPClient实例
     * @return FTPClient实例
     */
    public FTPClient getFtpClient() {
        return ftpClient;
    }
    
    /**
     * 检查是否连接
     * @return 是否已连接
     */
    public boolean isConnected() {
        return ftpClient != null && ftpClient.isConnected();
    }
    
    /**
     * SOCKS5代理SocketFactory实现
     * 用于为FTPClient提供通过SOCKS5代理的Socket连接
     */
    private static class Socks5SocketFactory extends SocketFactory {
        private final String proxyHost;
        private final int proxyPort;
        
        public Socks5SocketFactory(String proxyHost, int proxyPort) {
            this.proxyHost = proxyHost;
            this.proxyPort = proxyPort;
        }
        
        @Override
        public Socket createSocket() throws IOException {
            // 创建SOCKS5代理
            Proxy proxy = new Proxy(Proxy.Type.SOCKS, 
                new InetSocketAddress(proxyHost, proxyPort));
            return new Socket(proxy);
        }
        
        @Override
        public Socket createSocket(String host, int port) throws IOException {
            Socket socket = createSocket();
            socket.connect(new InetSocketAddress(host, port));
            return socket;
        }
        
        @Override
        public Socket createSocket(String host, int port, 
                                  java.net.InetAddress localAddr, int localPort) throws IOException {
            // SOCKS5代理不支持绑定本地地址，使用简化版本
            return createSocket(host, port);
        }
        
        @Override
        public Socket createSocket(java.net.InetAddress address, int port) throws IOException {
            return createSocket(address.getHostAddress(), port);
        }
        
        @Override
        public Socket createSocket(java.net.InetAddress address, int port,
                                  java.net.InetAddress localAddr, int localPort) throws IOException {
            // SOCKS5代理不支持绑定本地地址，使用简化版本
            return createSocket(address.getHostAddress(), port);
        }
    }
}
