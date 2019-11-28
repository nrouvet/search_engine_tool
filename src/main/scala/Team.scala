case class Team(var monsters : List[Monster]) {

  def this(){
    this(List.empty[Monster])
  }

  def addMonster(monster : Monster) : Unit = {
    this.monsters :+ monster
  }

  def dead(monster: Monster): Unit = {
    this.monsters diff List(monster)
  }

  def hasLost(): Boolean ={
    if(this.monsters.isEmpty) return true
     false
  }

}
