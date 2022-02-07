package simpledb;

import org.junit.Test;
import org.junit.Assert;

import simpledb.execution.Predicate.Op;
import simpledb.optimizer.IntHistogram;

import java.util.Random;

public class IntHistogramTest {

	/**
	 * Test to confirm that the IntHistogram implementation is constant-space
	 * (or, at least, reasonably small space; O(log(n)) might still work if
	 * your constants are good).
	 */
	@Test public void orderOfGrowthTest() {
		// Don't bother with a timeout on this test.
		// Printing debugging statements takes >> time than some inefficient algorithms.
		IntHistogram h = new IntHistogram(10000, 0, 100);
		
		// Feed the histogram more integers than would fit into our
		// 128mb allocated heap (4-byte integers)
		// If this fails, someone's storing every value...
		for (int c = 0; c < 33554432; c++) {
			h.addValue((c * 23) % 101);	// Pseudo-random number; at least get a distribution
		}
		
		// Try printing out all of the values; make sure "estimateSelectivity()"
		// cause any problems
		double selectivity = 0.0;
		for (int c = 0; c < 101; c++) {
			selectivity += h.estimateSelectivity(Op.EQUALS, c);
		}
		
		// All the selectivities should add up to 1, by definition.
		// Allow considerable leeway for rounding error, though 
		// (Java double's are good to 15 or so significant figures)
		Assert.assertTrue(selectivity > 0.99);
	}
	
	/**
	 * Test with a minimum and a maximum that are both negative numbers.
	 */
	@Test public void negativeRangeTest() {
		IntHistogram h = new IntHistogram(10, -60, -10);
		
		// All of the values here are negative.
		// Also, there are more of them than there are bins.
		for (int c = -60; c <= -10; c++) {
			h.addValue(c);
			h.estimateSelectivity(Op.EQUALS, c);
		}
		
		// Even with just 10 bins and 50 values,
		// the selectivity for this particular value should be at most 0.2.
		Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, -33) < 0.3);
		
		// And it really shouldn't be 0.
		// Though, it could easily be as low as 0.02, seeing as that's
		// the fraction of elements that actually are equal to -33.
		Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, -33) > 0.001);
	}
	
	/**
	 * Make sure that equality binning does something reasonable.
	 */
	@Test public void opEqualsTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		
		// This really should return "1.0"; but,
		// be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, 3) > 0.999);
		Assert.assertTrue(h.estimateSelectivity(Op.EQUALS, 8) < 0.001);
	}
	
	/**
	 * Make sure that GREATER_THAN binning does something reasonable.
	 */
	@Test public void opGreaterThanTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		h.addValue(1);
		h.addValue(10);
		
		// Be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, -1) > 0.999);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, 2) > 0.6);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, 4) < 0.4);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN, 12) < 0.001);
		Assert.assertEquals(0.8, h.estimateSelectivity(Op.GREATER_THAN, 2), 1e-2);
		Assert.assertEquals(0.2, h.estimateSelectivity(Op.GREATER_THAN, 3), 1e-2);
		Assert.assertEquals(0.2, h.estimateSelectivity(Op.GREATER_THAN, 4),1e-2 );
		Assert.assertEquals(0, h.estimateSelectivity(Op.GREATER_THAN, 12), 1e-2);
	}
	
	/**
	 * Make sure that LESS_THAN binning does something reasonable.
	 */
	@Test public void opLessThanTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		h.addValue(1);
		h.addValue(10);
		
		// Be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, -1) < 0.001);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, 2) < 0.4);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, 4) > 0.6);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN, 12) > 0.999);
	}
	
	/**
	 * Make sure that GREATER_THAN_OR_EQ binning does something reasonable.
	 */
	@Test public void opGreaterThanOrEqualsTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		h.addValue(1);
		h.addValue(10);
		
		// Be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, -1) > 0.999);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 2) > 0.6);
		Assert.assertEquals(0.8, h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 2), 1e-2);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 3) > 0.45);
		Assert.assertEquals(0.8, h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 3), 1e-2);
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 4) < 0.5);
		Assert.assertEquals(0.2, h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 4),1e-2 );
		Assert.assertTrue(h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 12) < 0.001);
		Assert.assertEquals(0, h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, 12), 1e-2);
	}
	
	/**
	 * Make sure that LESS_THAN_OR_EQ binning does something reasonable.
	 */
	@Test public void opLessThanOrEqualsTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		h.addValue(1);
		h.addValue(10);
		
		// Be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, -1) < 0.001);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 2) < 0.4);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 3) > 0.45);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 4) > 0.6);
		Assert.assertTrue(h.estimateSelectivity(Op.LESS_THAN_OR_EQ, 12) > 0.999);
	}
	
	/**
	 * Make sure that equality binning does something reasonable.
	 */
	@Test public void opNotEqualsTest() {
		IntHistogram h = new IntHistogram(10, 1, 10);
		
		// Set some values
		h.addValue(3);
		h.addValue(3);
		h.addValue(3);
		
		// Be conservative in case of alternate implementations
		Assert.assertTrue(h.estimateSelectivity(Op.NOT_EQUALS, 3) < 0.001);
		Assert.assertTrue(h.estimateSelectivity(Op.NOT_EQUALS, 8) > 0.01);
	}

	@Test public void all1Test() {
		IntHistogram h = new IntHistogram(100, 1, 100);
		for (int i = 1; i <= 100; ++i) {
			h.addValue(i);
		}
		Assert.assertEquals(0.01, h.estimateSelectivity(Op.EQUALS, 4), 1e-4);
		Assert.assertEquals(1, h.estimateSelectivity(Op.LESS_THAN, 101), 1e-4);
		Assert.assertEquals(0.49, h.estimateSelectivity(Op.LESS_THAN, 50), 1e-4);
		Assert.assertEquals(0, h.estimateSelectivity(Op.LESS_THAN, 1), 1e-4);
		Assert.assertEquals(0.01, h.estimateSelectivity(Op.LESS_THAN, 2), 1e-4);
		// indivisible
		IntHistogram h1 = new IntHistogram(101, 1, 102);
		for (int i = 1; i <= 102; ++i) {
			h1.addValue(i);
		}
		Assert.assertEquals(0.01, h1.estimateSelectivity(Op.EQUALS, 4), 1e-4);
		Assert.assertEquals(1, h1.estimateSelectivity(Op.LESS_THAN, 102), 1e-4);
		Assert.assertEquals(0.49, h1.estimateSelectivity(Op.LESS_THAN, 51), 1e-4);
		Assert.assertEquals(0, h1.estimateSelectivity(Op.LESS_THAN, 1), 1e-4);
		Assert.assertEquals(0.01, h1.estimateSelectivity(Op.LESS_THAN, 2), 1e-4);
	}

	// check internal helper function
	@Test public void intervalTest() {
		// simplest case
		IntHistogram h = new IntHistogram(10, 1, 100);
		for (int i = 0; i < 10; ++i) {
			Assert.assertEquals(i * 10 + 1, h.getIntervalMin(i));
			Assert.assertEquals((i + 1) * 10 + 1, h.getIntervalMax(i));
		}
		// each bucket contains one number
		IntHistogram h1 = new IntHistogram(17, 0, 16);
		for (int i = 0; i < 17; ++i) {
			Assert.assertEquals(i, h1.getIntervalMin(i));
			Assert.assertEquals(i + 1, h1.getIntervalMax(i));
		}
		// not integer
		IntHistogram h2 = new IntHistogram(17, 0, 19);
		for (int i = 0; i < 17; ++i) {
			Assert.assertEquals(i, h2.getIntervalMin(i));
			Assert.assertEquals(i + 1, h2.getIntervalMax(i));
		}
	}

	// lots of input
	@Test public void largeTest() {
		final int maxVal = 43;
		final int count = 7512311;
		IntHistogram h = new IntHistogram(101, 0, maxVal);
		int cur = 0;
		for (int i = 0; i < count; ++i) {
			h.addValue(cur);
			// cur : [0, maxVal]
			cur = (cur + 1) % (maxVal + 1);
		}
		double delta = 2d / maxVal;
		for (int i = 0; i < count; ++i) {
			double frac = Math.random();
			Assert.assertEquals(frac, h.estimateSelectivity(Op.LESS_THAN, (int)(maxVal * frac)), delta);
		}
	}

	/**
	 * Mimics the test in TableStatsTest
	 */
	@Test public void randomTest() {
		int maxVal = 32;
		int count = 10200;
		IntHistogram h = new IntHistogram(100, 0, maxVal);
		Random ran = new Random();
		for (int i = 0; i < count; ++i) {
			int val = ran.nextInt(maxVal + 1);
			h.addValue(val);
		}
		double delta = 2d / maxVal;
		for (int i = 0; i < count; ++i) {
			double frac = Math.random();
			Assert.assertEquals(frac, h.estimateSelectivity(Op.LESS_THAN_OR_EQ, (int)(maxVal * frac)), delta);
		}
		Assert.assertEquals(1 / (double)maxVal, h.estimateSelectivity(Op.EQUALS, maxVal / 2), delta);
		Assert.assertEquals(0.5, h.estimateSelectivity(Op.LESS_THAN, maxVal / 2), delta);
		Assert.assertEquals(0.5, h.estimateSelectivity(Op.LESS_THAN_OR_EQ, maxVal / 2), delta);
		Assert.assertEquals(0.5, h.estimateSelectivity(Op.GREATER_THAN, maxVal / 2), delta);
		Assert.assertEquals(0.5, h.estimateSelectivity(Op.GREATER_THAN_OR_EQ, maxVal / 2), delta);
	}
}
