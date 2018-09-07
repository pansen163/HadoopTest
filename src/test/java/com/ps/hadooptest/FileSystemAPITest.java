package com.ps.hadooptest;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

import org.junit.Test;

/**
 * Created by pansen on 2018/9/7.
 */
public class FileSystemAPITest extends TestCase {

  private static final String BASE_URL = "hdfs://localhost:8082";

  /**
   * 追加到文件
   */
  @Test
  public void testAppend() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(BASE_URL + "/1.txt"), conf);
    fs.append(new Path(BASE_URL + "/user/root/input/2.txt"));
    System.out.println();
  }

  /**
   * 本地文件上传测试
   */
  @Test
  public void testCopyFromLocalFile() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
    fs.copyFromLocalFile(new Path("/tmp/756546.txt"),
                         new Path(BASE_URL + "/demo1"));
  }

  /**
   * 从hdfs下载文件到本地
   */
  @Test
  public void testCopyToLocalFile() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
    fs.copyToLocalFile(new Path(BASE_URL + "/user/root/input/3.txt"),
                       new Path("C:\\Users\\lenovo\\Desktop\\4.txt"));
  }

  /**
   * 测试文件创建
   */
  @Test
  public void testCreate() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
    Path path = new Path("/user/root/input/4.txt");
    FSDataOutputStream out = fs.create(path, true);
    out.write("hello hadoop".getBytes());
  }

  /**
   * 创建空文件
   */
  @Test
  public void testCreateNewFile() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
    Path path = new Path("/user/root/input/5.txt");
    fs.createNewFile(path);
  }

  /**
   * 删除文件
   */
  @Test
  public void testDelete() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
    Path path = new Path("/user/root/input/5.txt");
    fs.delete(path);
  }

  /**
   * 文件是否存在
   */
  @Test
  public void testExists() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
    Path path = new Path("/user/root/input/5.txt");
    System.out.println(fs.exists(path));
  }

  /**
   * 文件状态
   */
  @Test
  public void testGetFileStatus() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
    Path path = new Path("/user/root/input/1.txt");
    FileStatus status = fs.getFileStatus(path);
    System.out.println(status.getModificationTime());
  }

  /**
   * 文件状态，可以获得文件夹信息
   */
  @Test
  public void testListFile() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
    Path path = new Path("/user/root/input");
    FileStatus[] status = fs.listStatus(path);
    for (FileStatus fileStatus : status) {
      System.out.println(fileStatus.getPath());
    }
  }

  /**
   * 创建文件夹
   */
  @Test
  public void testMkdirs() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
    Path path = new Path("/user/root/input/sub");
    fs.mkdirs(path);
  }

  /**
   * 移动本地文件到hdfs
   */
  @Test
  public void testMoveFromLocalFile() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
    Path path = new Path("/user/root/input/sub");
    fs.moveFromLocalFile(new Path("C:\\Users\\lenovo\\Desktop\\4.txt"), path);
  }

  /**
   * 重命名
   */
  @Test
  public void testRename() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(URI.create(BASE_URL), conf);
    Path path = new Path("/user/root/input/sub/4.txt");
    Path dest = new Path("/user/root/input/sub/4_1.txt");
    fs.rename(path, dest);
  }
}