import scala.collection.mutable.ListBuffer

case class Team(var monsters : List[Monster]) {

  def this(){
    this(List.empty[Monster])
  }

  def addMonster(monster : Monster) : Unit = {
    var buffer = new ListBuffer[Monster]()
    this.monsters.foreach( x => buffer += x)
    buffer += monster
    this.monsters = buffer.toList
  }

  def dead(monster: Monster): Unit = {
    var buffer = this.monsters.toBuffer
    buffer -= monster
    this.monsters = buffer.toList
  }

  def hasLost(): Boolean ={
    if(monsters.isEmpty) true
    else false
  }

}
