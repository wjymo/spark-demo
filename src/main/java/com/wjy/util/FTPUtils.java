package com.wjy.util;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;

public class FTPUtils {
    private static FTPClient ftpClient=new FTPClient();
    /** 本地字符编码 */
    private static String LOCAL_CHARSET = "GBK";
    public static Boolean upload(String remoteName,String localPath){
        try {
            //设置连接地址和端口
            ftpClient.connect("wang-109", 21);
            //设置用户和密码
            ftpClient.login("testftp2", "123");
            //设置文件类型
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            if (FTPReply.isPositiveCompletion(ftpClient.sendCommand("OPTS UTF8", "ON"))) {
                // 开启服务器对UTF-8的支持，如果服务器支持就用UTF-8编码，否则就使用本地编码（GBK）.
                LOCAL_CHARSET = "UTF-8";
            }
            ftpClient.setControlEncoding(LOCAL_CHARSET);
            //上传
            boolean flag = ftpClient.storeFile(remoteName,new FileInputStream(new File(localPath)));
            return flag;
        } catch (SocketException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            //关闭连接
            if(null != ftpClient) {
                try {
                    ftpClient.disconnect();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }
}
