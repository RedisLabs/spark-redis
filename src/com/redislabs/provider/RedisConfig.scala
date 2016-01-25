package com.redislabs.provider


import com.redislabs.provider.redis.NodesInfo._

class RedisConfig(ip: String, port: Int, password: String = null) extends Serializable {
  val nodes: java.util.ArrayList[(String, Int, String)] = new java.util.ArrayList[(String, Int, String)]

  getNodes((ip, port, password)).foreach(x => nodes.add((x._1, x._2, x._5)))

  /**
   *
   * @param sPos start position of a slots range
   * @param ePos end position of a slots range
   * @return list of nodes(addr, port, index, range, startSlot, endSlot), where Inter([startSlot, endSlot], [sPos, ePos]) is not Nil(master only now)
   */
  def getNodesBySlots(sPos: Int, ePos: Int) = {
    def inter(sPos1: Int, ePos1: Int, sPos2: Int, ePos2: Int):Boolean = {
      if (sPos1 <= sPos2)
        return ePos1 >= sPos2
      else
        return ePos2 >= sPos1
    }
    val node = nodes.get(scala.util.Random.nextInt().abs % nodes.size())
    getSlots((node._1, node._2, node._3)).filter(node => inter(sPos, ePos, node._5, node._6)).filter(_._3 == 0) //master only now
  }
}
