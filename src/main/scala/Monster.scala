case class Monster(var id : Int, var name : String, var equipe : String, var armure: Int, var HP : Int ) {


  def Damage (reduceHP : Int): Unit ={
    this.HP -= reduceHP
  }
  def Heal (increaseHP : Int): Unit ={
    this.HP += increaseHP
  }
  def Attack(target : Monster , sort: Sort ): Unit ={
    if(sort.typeSort == false)
    target.Damage(sort.power)
    else
      target.Heal(sort.power)
  }





}

