package org.duo.hdfs_api;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class HdfsApiDemo {


    @Test
    public void urlHdfs() throws IOException {

        //1:注册url
        URL.setURLStreamHandlerFactory( new FsUrlStreamHandlerFactory());
        //2:获取hdfs文件的输入流
        InputStream inputStream = new URL("hdfs://server01:8020/a.txt").openStream();

        //3:获取本地文件的输出流
        FileOutputStream outputStream = new FileOutputStream(new File("D:\\hello2.txt"));

        //4:实现文件的拷贝
        IOUtils.copy(inputStream, outputStream);

        //5:关流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);

    }
}
