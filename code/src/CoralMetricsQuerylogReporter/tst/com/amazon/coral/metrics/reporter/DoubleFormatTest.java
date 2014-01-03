package com.amazon.coral.metrics.reporter;

import org.junit.Test;
import static org.junit.Assert.*;

import java.text.NumberFormat;

public class DoubleFormatTest {

  private void test(double test, String expected) {
    test(test, expected, 6);
  }

  private void test(double test, String expected, int prec) {
    test(test, expected, 0, prec);
  }

  private void test(double test, String expected, int minDigits, int maxDigits) {
    DoubleFormat df = new DoubleFormat();

    df.setMinimumFractionDigits(minDigits);
    df.setMaximumFractionDigits(maxDigits);

    StringBuilder sb = new StringBuilder();
    df.format(sb, test);
    String formatted = sb.toString();

    // uncomment this section to show tests/results
//      if(expected.equals(formatted) == false) {
//        System.out.print("!! ");
//      } else {
//        System.out.print("   ");
//      }
//      System.out.println("in=" + test + " minD=" + minDigits + " maxD=" + maxDigits + " exp=" + expected + " actual=" + formatted);

    assertEquals(expected, formatted);
  }

  @Test
  public void positive() {
    test(12345.6789, "12345.6789");
  }

  @Test
  public void negative() {
    test(-12345.6789, "-12345.6789");
  }

  @Test
  public void lessThreshold() {
    test(9.87654321, "9.876543");
  }

  @Test
  public void roundUp() {
    test(0.9999996, "1");
  }

  @Test
  public void edge() {
    test(0.9999995, "1");
  }

  @Test
  public void edge2() {
    test(0.95, "1", 1);
  }

  @Test
  public void edge2b() {
    test(1.85, "1.8", 1);
  }

  @Test
  public void edge2c() {
    test(0.05, "0.1", 1);
  }

  @Test
  public void roundDown() {
    test(0.9999994, "0.999999");
  }

  @Test
  public void zero() {
    test(0.0000, "0");
  }

  @Test
  public void fraction() {
    test(0.123, "0.123");
  }

  @Test
  public void integer() {
    test(1.0000, "1");
  }

  @Test
  public void prec() {
    test(1.23456789, "1.23", 2);
  }

  @Test
  public void leadingZerosInDecimal() {
    test(1.000123, "1.000123");
  }

  @Test
  public void minDigits() {
    test(1.1, "1.100", 3, 6);
  }

  @Test
  public void trailingZeros() {
    test(1.0, "1.000000", 6, 6);
  }

  @Test
  public void maxDigits() {
    test(1.123456789, "1.12", 2, 2);
  }

  @Test(expected=IllegalArgumentException.class)
  public void invalidMinimumDigits() {
    DoubleFormat df = new DoubleFormat();
    df.setMinimumFractionDigits(-1);
  }

  @Test(expected=IllegalArgumentException.class)
  public void invalidMinimumDigits2() {
    DoubleFormat df = new DoubleFormat();
    df.setMinimumFractionDigits(9);
  }

  @Test
  public void validMinimumDigits() {
    DoubleFormat df = new DoubleFormat();
    df.setMinimumFractionDigits(1);
  }

  @Test(expected=IllegalArgumentException.class)
  public void invalidMaximumDigits() {
    DoubleFormat df = new DoubleFormat();
    df.setMaximumFractionDigits(-1);
  }

  @Test(expected=IllegalArgumentException.class)
  public void invalidMaximumDigits2() {
    DoubleFormat df = new DoubleFormat();
    df.setMaximumFractionDigits(9);
  }

  @Test
  public void validMaximumDigits() {
    DoubleFormat df = new DoubleFormat();
    df.setMaximumFractionDigits(1);
  }

  @Test(expected=IllegalStateException.class)
  public void invalidMinMaxDigitsState() {
    DoubleFormat df = new DoubleFormat();
    df.setMinimumFractionDigits(6);
    df.setMaximumFractionDigits(3);
    StringBuilder sb = new StringBuilder();
    df.format(sb, 1.0);
  }

  @Test
  public void defaultMaximumDigits() {
    assertEquals(8, new DoubleFormat().getMaximumFractionDigits());
  }

  @Test
  public void defaultMinimumDigits() {
    assertEquals(0, new DoubleFormat().getMinimumFractionDigits());
  }

  @Test
  public void tooBig() {
    NumberFormat format = NumberFormat.getInstance();
    format.setMinimumFractionDigits(0);
    format.setMaximumFractionDigits(9);
    format.setGroupingUsed(false);

    double big = Double.MAX_VALUE;
    String expected = format.format(big);
    DoubleFormat df = new DoubleFormat();

    StringBuilder sb = new StringBuilder();
    df.format(sb, big);

    assertEquals(expected, sb.toString());
  }

  // This test case disabled because it can take upwards of 5 seconds to run...
  //@Test
  public void benchmark() {
    int i;
    long startTime, endTime;
    long duration_JavaFormat, duration_DoubleFormat, duration_JavaStringBuilder;
    int iterations = 1000000;

    double testNumber = 16570.1251473;

    NumberFormat javaFormatter = NumberFormat.getInstance();
    javaFormatter.setMinimumFractionDigits(0);
    javaFormatter.setMaximumFractionDigits(6);
    javaFormatter.setGroupingUsed(false);

    DoubleFormat doubleFormat = new DoubleFormat();
    doubleFormat.setMinimumFractionDigits(0);
    doubleFormat.setMaximumFractionDigits(6);

    StringBuilder sb = new StringBuilder();


    startTime = System.currentTimeMillis();

    for(i = 0; i < iterations; i++)
      sb.append(testNumber);

    endTime = System.currentTimeMillis();
    duration_JavaStringBuilder = endTime - startTime;


    startTime = System.currentTimeMillis();

    for(i = 0; i < iterations; i++)
      javaFormatter.format(testNumber);

    endTime = System.currentTimeMillis();
    duration_JavaFormat = endTime - startTime;



    startTime = System.currentTimeMillis();

    for(i = 0; i < iterations; i++)
      doubleFormat.format(new StringBuilder(), testNumber);

    endTime = System.currentTimeMillis();
    duration_DoubleFormat = endTime - startTime;


    System.out.println("DoubleFormat Performance comparison: " + iterations +" iterations");
    System.out.println("\tJava StringBuilder: " + duration_JavaStringBuilder + " ms");
    System.out.println("\tJava NumberFormat:  " + duration_JavaFormat + " ms");
    System.out.println("\tDoubleFormat:       " + duration_DoubleFormat + " ms");
  }

  @Test
  public void leadingZerosIntegerOverflow() {
    test(0.6972461198092492,    "0.69724612", 8);
    test(0.3957474025628186,    "0.3957474", 8);
    test(0.22607775429405264,   "0.22607775", 8);
    test(0.3291082710169503,    "0.32910827", 8);
    test(0.32921492069249947,   "0.32921492", 8);
    test(0.18868304981374573,   "0.18868305", 8);
    test(0.10704062259380864,   "0.10704062", 8);
    test(0.5517452296495586,    "0.55174523", 8);
    test(0.6763267440490248,    "0.67632674", 8);
    test(0.6644974991174629,    "0.6644975", 8);
    test(0.0058853381799743865, "0.00588534", 8);
    test(0.23972173153373955,   "0.23972173", 8);
    test(0.3000722792766619,    "0.30007228", 8);
    test(0.37517415521245345,   "0.37517416", 8);
    test(0.012308646565343029,  "0.01230865", 8);
    test(0.06245844233466158,   "0.06245844", 8);
    test(0.8257932602390585,    "0.82579326", 8);
    test(0.3542880017571749,    "0.354288", 8);
    test(0.08525139832896267,   "0.0852514", 8);
    test(0.13705051900190313,   "0.13705052", 8);
    test(0.7028752585515867,    "0.70287526", 8);
    test(0.7179510797870567,    "0.71795108", 8);
    test(0.7601627632324963,    "0.76016276", 8);
    test(0.5748998978654571,    "0.5748999", 8);
    test(0.16737961281134406,   "0.16737961", 8);
    test(0.7665142427379052,    "0.76651424", 8);
    test(0.3717073105453246,    "0.37170731", 8);
    test(0.6835382112082834,    "0.68353821", 8);
    test(0.28727209275905263,   "0.28727209", 8);
  }
}
