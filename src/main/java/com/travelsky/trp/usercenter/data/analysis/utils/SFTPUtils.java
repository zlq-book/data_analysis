package com.travelsky.trp.usercenter.data.analysis.utils;

import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;
import java.util.Vector;

/**
 * SFTP工具类
 * 基于JSch实现的SFTP连接和文件操作工具
 */
public class SFTPUtils {

    private static final Logger logger = LoggerFactory.getLogger(SFTPUtils.class);
    private final String host;
    private final int port;
    private final String username;
    private final String password;
    private Session session;
    private ChannelSftp sftpChannel;

    /**
     * 构造函数
     *
     * @param host     SFTP服务器地址
     * @param port     SFTP服务器端口
     * @param username 用户名
     * @param password 密码
     */
    public SFTPUtils(String host, int port, String username, String password) {
        this.host = host;
        this.port = port;
        this.username = username;
        this.password = password;
    }

    /**
     * 构造函数，使用默认端口22
     *
     * @param host     SFTP服务器地址
     * @param username 用户名
     * @param password 密码
     */
    public SFTPUtils(String host, String username, String password) {
        this(host, 22, username, password);
    }

    /**
     * 连接SFTP服务器
     *
     * @return 是否连接成功
     */
    public boolean connect() {
        try {
            JSch jsch = new JSch();
            session = jsch.getSession(username, host, port);
            session.setPassword(password);
            //本地调试需要用代理
//            Proxy proxy = new ProxySOCKS5("127.0.0.1",1080);
//            session.setProxy(proxy);
            // 设置连接配置
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no"); // 忽略host key检查
            config.put("UserKnownHostsFile", "/dev/null");
            session.setConfig(config);

            // 连接
            session.connect(30000); // 30秒超时
            logger.info("SFTP Session连接成功: {}:{}", host, port);

            // 打开SFTP通道
            Channel channel = session.openChannel("sftp");
            channel.connect();
            sftpChannel = (ChannelSftp) channel;

            logger.info("SFTP通道连接成功: {}:{}", host, port);
            return true;

        } catch (JSchException e) {
            logger.error("SFTP连接失败: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * 断开SFTP连接
     */
    public void disconnect() {
        try {
            if (sftpChannel != null && sftpChannel.isConnected()) {
                sftpChannel.disconnect();
                logger.info("SFTP通道已断开");
            }
            if (session != null && session.isConnected()) {
                session.disconnect();
                logger.info("SFTP Session已断开");
            }
        } catch (Exception e) {
            logger.error("断开SFTP连接时发生错误: {}", e.getMessage(), e);
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
            sftpChannel.cd(dirPath);
            logger.info("切换SFTP工作目录成功: {}", dirPath);
            return true;
        } catch (SftpException e) {
            logger.error("切换SFTP工作目录失败: {}, 错误: {}", dirPath, e.getMessage());
            return false;
        }
    }

    /**
     * 获取当前工作目录
     *
     * @return 当前工作目录路径
     */
    public String getCurrentWorkingDirectory() {
        try {
            String currentDir = sftpChannel.pwd();
            logger.info("当前SFTP工作目录: {}", currentDir);
            return currentDir;
        } catch (SftpException e) {
            logger.error("获取当前SFTP工作目录失败: {}", e.getMessage());
            return null;
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
            sftpChannel.stat(filePath);
            return true;
        } catch (SftpException e) {
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
    public Vector<ChannelSftp.LsEntry> listFiles() {
        try {
            return sftpChannel.ls(".");
        } catch (SftpException e) {
            logger.error("列出SFTP目录文件失败: {}", e.getMessage());
            return new Vector<>();
        }
    }

    /**
     * 列出指定目录的文件
     *
     * @param dirPath 目录路径
     * @return 文件列表
     */
    @SuppressWarnings("unchecked")
    public Vector<ChannelSftp.LsEntry> listFiles(String dirPath) {
        try {
            return sftpChannel.ls(dirPath);
        } catch (SftpException e) {
            logger.error("列出SFTP目录文件失败: {}, 错误: {}", dirPath, e.getMessage());
            return new Vector<>();
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
            InputStream inputStream = sftpChannel.get(filePath);
            logger.info("获取SFTP文件流成功: {}", filePath);
            return inputStream;
        } catch (SftpException e) {
            logger.error("获取SFTP文件流失败: {}, 错误: {}", filePath, e.getMessage());
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
        try {
            sftpChannel.get(remoteFilePath, localFilePath);
            logger.info("SFTP文件下载成功: {} -> {}", remoteFilePath, localFilePath);
            return true;
        } catch (SftpException e) {
            logger.error("SFTP文件下载失败: {} -> {}, 错误: {}", remoteFilePath, localFilePath, e.getMessage());
            return false;
        }
    }

    /**
     * 上传文件
     *
     * @param localFilePath  本地文件路径
     * @param remoteFilePath 远程文件路径
     * @return 是否上传成功
     */
    public boolean uploadFile(String localFilePath, String remoteFilePath) {
        try {
            sftpChannel.put(localFilePath, remoteFilePath);
            logger.info("SFTP文件上传成功: {} -> {}", localFilePath, remoteFilePath);
            return true;
        } catch (SftpException e) {
            logger.error("SFTP文件上传失败: {} -> {}, 错误: {}", localFilePath, remoteFilePath, e.getMessage());
            return false;
        }
    }

    /**
     * 删除文件
     *
     * @param filePath 远程文件路径
     * @return 是否删除成功
     */
    public boolean deleteFile(String filePath) {
        try {
            sftpChannel.rm(filePath);
            logger.info("SFTP文件删除成功: {}", filePath);
            return true;
        } catch (SftpException e) {
            logger.error("SFTP文件删除失败: {}, 错误: {}", filePath, e.getMessage());
            return false;
        }
    }

    /**
     * 创建目录
     *
     * @param dirPath 目录路径
     * @return 是否创建成功
     */
    public boolean createDirectory(String dirPath) {
        try {
            sftpChannel.mkdir(dirPath);
            logger.info("SFTP目录创建成功: {}", dirPath);
            return true;
        } catch (SftpException e) {
            logger.error("SFTP目录创建失败: {}, 错误: {}", dirPath, e.getMessage());
            return false;
        }
    }

    /**
     * 删除目录
     *
     * @param dirPath 目录路径
     * @return 是否删除成功
     */
    public boolean removeDirectory(String dirPath) {
        try {
            sftpChannel.rmdir(dirPath);
            logger.info("SFTP目录删除成功: {}", dirPath);
            return true;
        } catch (SftpException e) {
            logger.error("SFTP目录删除失败: {}, 错误: {}", dirPath, e.getMessage());
            return false;
        }
    }

    /**
     * 检查是否连接
     *
     * @return 是否已连接
     */
    public boolean isConnected() {
        return session != null && session.isConnected()
               && sftpChannel != null && sftpChannel.isConnected();
    }

    /**
     * 获取SFTP通道
     *
     * @return SFTP通道
     */
    public ChannelSftp getSftpChannel() {
        return sftpChannel;
    }
}