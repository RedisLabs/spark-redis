package com.redislabs.provider


import com.redislabs.provider.redis.ClusterInfo

//
//class RedisConfig(val ip: String, val port: Int) extends Serializable {
//  val clusterInfo = new ClusterInfo(ip, port)
//
//  //getNodes((ip, port)).foreach(x => nodes.add((x._1, x._2)))
//
//  /**
//   *
//   * @param sPos start position of a slots range
//   * @param ePos end position of a slots range
//   * @return list of nodes(addr, port, index, range, startSlot, endSlot), where
//   *         Inter([startSlot, endSlot], [sPos, ePos]) is not Nil(master only now)
//   */
//  def getNodesBySlots(sPos: Int, ePos: Int): Array[(String, Int, Int, Int, Int, Int)] = {
//    def inter(sPos1: Int, ePos1: Int, sPos2: Int, ePos2: Int) =
//      if (sPos1 <= sPos2) ePos1 >= sPos2 else ePos2 >= sPos1
//
//    val node = clusterInfo.getRandomNode
//    clusterInfo.slots.filter(node => inter(sPos, ePos, node._5, node._6)).
//      filter(_._3 == 0) //master only now
//  }
//}
