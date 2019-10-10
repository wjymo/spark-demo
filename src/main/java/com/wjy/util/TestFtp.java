package com.wjy.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.SocketException;
import java.net.URL;

public class TestFtp {
    private static FTPClient ftpClient=null;
    static {
        //创建ftp客户端
        ftpClient = new FTPClient();
    }
    public static void main(String[] args) {
//        testFtp();
        downloadHttpUrl();
    }

    private static void testFtp() {

        try {
            //设置连接地址和端口
            ftpClient.connect("wang-109", 21);
            //设置用户和密码
            ftpClient.login("testftp2", "123");
            //设置文件类型
            ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
            //上传
            boolean flag = ftpClient.storeFile("xx2.jpg",
                    new FileInputStream(new File("D:\\images\\xx2.jpg")));
            if(flag)
                System.out.println("上传成功...");
            else
                System.out.println("上传失败...");
//            FTPFile[] ftpFiles = ftpClient.listFiles();
            System.out.println(1);
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
    }


    public static void downloadHttpUrl() {
        try {
//            URL httpurl = new URL("https://photo.zastatic.com/images/photo/407936/1631740418/23305481226310316.jpg?scrop=1&amp;crop=1&amp;cpos=north&amp;w=200&amp;h=200");
//            File dirfile = new File("D:\\images");
//            if (!dirfile.exists()) {
//                dirfile.mkdirs();
//            }
//            FileUtils.copyURLToFile(httpurl, new File("D:\\images"+"\\xx.jpg"));
            File file = new File("D:\\images\\过客-1097681757.jpg");
            file.delete();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
