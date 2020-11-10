/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection


import scala.collection.mutable

class LfuCache[KeyType, ItemType] extends mutable.Iterable[LfuItem[KeyType, ItemType]] {


<<<<<<< Updated upstream
  private val byKey: mutable.HashMap[KeyType, LfuItem[KeyType, ItemType]] = mutable.HashMap[KeyType, LfuItem[KeyType, ItemType]]()
=======
  private def byKey(key: KeyType): Option[LfuItem[KeyType, ItemType]]= {
    theMap.get(key) match {
      case Some(item) => Some(item)
      case _ => None
    }
  }

  //private val byKey: mutable.HashMap[KeyType, LfuItem[KeyType, ItemType]] = mutable.HashMap[KeyType, LfuItem[KeyType, ItemType]]()
>>>>>>> Stashed changes
  val frequencyHead: FrequencyNode[KeyType, ItemType] = FrequencyNode(-1)
  // TODO: Nullpointer Exception
  def removeNode(node: FrequencyNode[KeyType, ItemType]): Unit = {
    if (node.nextNode != null) {
      node.prev.nextNode = node.nextNode
      node.nextNode.prev = node.prev
      node.nextNode = null
      node.prev = null
    } else if (node.nextNode == null) {
      node.prev.nextNode = null
      node.nextNode = null
      node.prev = null
    }
  }

  def remove(key: KeyType): ItemType = {
    val temp = this.byKey(key)
<<<<<<< Updated upstream
    if (temp == null) {
      return temp.data
=======
    temp match {
      case Some(x) =>
        val thisRoot = x.parent
        thisRoot.items -= x
        if (thisRoot.items.isEmpty) {
          this.removeNode(thisRoot)
        }
        Some(this.theMap.remove(key).get.data)
      case _ => None
>>>>>>> Stashed changes
    }
    val thisRoot = temp.parent
    this.removeNode(thisRoot)
    this.byKey.remove(key).get.data
  }

  def get(key: KeyType): ItemType = {
    val tmp = this.byKey(key)
<<<<<<< Updated upstream
    if (tmp == null) {
      return tmp.data
    }

    val freq = tmp.parent
    var nextFreq = freq.nextNode

    if ((nextFreq == this.frequencyHead) || nextFreq.value != (freq.value + 1)) {
      nextFreq = LfuCache.getNewNode(freq, nextFreq, freq.value + 1)
=======
    tmp match {
      case Some(x) =>
        val freq = x.parent
        var nextFreq = freq.nextNode
        // TODO: Nullpointer Exception
        if ((nextFreq == this.frequencyHead || nextFreq == null || nextFreq.value != (freq.value + 1))) {
          nextFreq = LfuCache.getNewNode(freq, nextFreq, freq.value + 1)
        }
        nextFreq.items += x
        x.parent = nextFreq

        nextFreq.prev.items -= nextFreq.prev.items.filter(freq => freq.key == key).head
        if (nextFreq.prev.items.isEmpty) {
          this.removeNode(freq)
        }
        Some(x.data)
      case _ =>
        None
>>>>>>> Stashed changes
    }

    nextFreq.items += this.byKey(key)
    tmp.parent = nextFreq

    nextFreq.prev.items -= nextFreq.prev.items.filter(freq => freq.key == key).head
    if (nextFreq.prev.items.isEmpty) {
      this.removeNode(freq)
    }
    tmp.data
  }

  def put(key: KeyType, value : ItemType) = {
    if (this.byKey.contains(key)) {
      throw new Exception("Key already exists")
    }

    val freqNode = this.frequencyHead.nextNode
    if (freqNode.value != 1){
      this.frequencyHead.nextNode = LfuCache.getNewNode(this.frequencyHead, freqNode)
      this.byKey(key) = LfuItem[KeyType, ItemType](this.frequencyHead.nextNode, value, key)
      this.frequencyHead.nextNode.items += this.byKey(key)
    }else{
      this.byKey(key) = LfuItem(freqNode, value, key)
      freqNode.items += this.byKey(key)
    }
  }

  def containsKey(key: KeyType): Boolean = {
    byKey contains  key
  }


  def clear(): Unit = {
    byKey.clear()
  }


  def getLfuItem(): Option[LfuItem[KeyType, ItemType]] = {
    if (this.byKey.isEmpty) {
      Some(None)
    }
<<<<<<< Updated upstream
    Some(this.byKey(this.frequencyHead.nextNode.items.head.key))
=======
    try {
      this.byKey(this.frequencyHead.nextNode.items.head.key)
    } catch {
      case e: java.util.NoSuchElementException => None
    }
>>>>>>> Stashed changes
  }

  override def iterator: Iterator[LfuItem[KeyType, ItemType]] = new Iterator[LfuItem[KeyType, ItemType]]{
    def hasNext: Boolean =
      getLfuItem() match  {
        case Some(_) => true
        case _ => false
      }

    def next: LfuItem[KeyType, ItemType] = {
      getLfuItem() match {
        case Some(x) => x
      }
    }
  }
}

case class LfuItem[KeyType, ItemType](var parent: FrequencyNode[KeyType, ItemType], data : ItemType, key: KeyType){
  def getKey = key
  def getValue = data
}

case class FrequencyNode[KeyType, ItemType](value: Int = 1) extends mutable.Iterable[FrequencyNode[KeyType, ItemType]] {
  var items: mutable.ArrayBuffer[LfuItem[KeyType, ItemType]] = mutable.ArrayBuffer[LfuItem[KeyType, ItemType]] ()
  var prev: FrequencyNode[KeyType, ItemType] = this
  var nextNode: FrequencyNode[KeyType, ItemType] = this

  override def iterator: Iterator[FrequencyNode[KeyType, ItemType]] = new Iterator[FrequencyNode[KeyType, ItemType]] {
    def hasNext: Boolean = nextNode != null
    def next(): FrequencyNode[KeyType, ItemType] = nextNode
  }

}

object LfuCache {
  def getNewNode[KeyType, ItemType](prev: FrequencyNode[KeyType, ItemType], nextNode: FrequencyNode[KeyType, ItemType], freqValue: Int = 1): FrequencyNode[KeyType, ItemType] = {
    val node = FrequencyNode[KeyType, ItemType](freqValue)
    node.prev = prev
    node.nextNode = nextNode
    node.prev.nextNode = node
    node.nextNode.prev = node
    node
  }
}


object main extends App {
  val testlfu = new LfuCache[Int, Long]
  testlfu.put(1, 100)
  testlfu.put(2, 200)
  testlfu.put(3, 300)
  testlfu.put(4, 400)

  testlfu.get(1)
  testlfu.get(2)
  testlfu.get(3)

  testlfu.get(1)
  testlfu.get(2)
  println(testlfu.getLfuItem())

  testlfu.remove(4)
  println(testlfu.getLfuItem())
}

