package io.transwarp.test;

/**
 * Created by root on 7/14/17.
 */
public class ExceptionTest {
  public static void main(String[] args) {
    String id = "";
    try {
      id = "1";
      throw new Exception("haha");
    } catch (Exception e) {
      System.out.println("Error msg is: " + e.getMessage());
    }
    System.out.println("id = " + id);
  }
}
