package com.honta.trembita.parallel

import java.lang.reflect.{Array => JArray}
import java.util.concurrent.{ForkJoinPool, ForkJoinTask, RecursiveAction}

import scala.reflect.ClassTag


@SerialVersionUID(-749935388568367268L)
protected [trembita]object MergeSort {
  def sort[T: Ordering](a: Array[T])(implicit ctg: ClassTag[T]): Unit = {
    @SuppressWarnings(Array("unchecked"))
    val helper = JArray.newInstance(ctg.runtimeClass, a.length).asInstanceOf[Array[T]]
    val forkJoinPool = ForkJoinPool.commonPool()
    forkJoinPool.invoke(new MergeSort(a, helper, 0, a.length - 1))
  }
}
@SerialVersionUID(-749935388568367268L)
protected [trembita]class MergeSort[T: Ordering](val a: Array[T], val helper: Array[T], val lo: Int, val hi: Int) extends RecursiveAction {
  override protected def compute(): Unit = {
    if (lo >= hi) return
    val mid = lo + (hi - lo) / 2
    val left = new MergeSort[T](a, helper, lo, mid)
    val right = new MergeSort[T](a, helper, mid + 1, hi)
    ForkJoinTask.invokeAll(left, right)
    merge(this.a, this.helper, this.lo, mid, this.hi)
  }
  private def merge(a: Array[T], helper: Array[T], lo: Int, mid: Int, hi: Int): Unit = {
    System.arraycopy(a, lo, helper, lo, hi + 1 - lo)
    var i = lo
    var j = mid + 1
    var k = lo
    while (k <= hi) {
      if (i > mid) {
        a(k) = helper(j)
        j += 1
      } else if (j > hi) {
        a(k) = helper(i)
        i += 1
      } else if (isLess(helper(i), helper(j))) {
        a(k) = helper(i)
        i += 1
      } else {
        a(k) = helper(j)
        j += 1
      }
      k += 1
    }
  }
  private def isLess(a: T, b: T) = Ordering[T].lt(a, b)
}