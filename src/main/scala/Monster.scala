class Monster(var id : Int, var name : String, var equipe : String, var armure: Int, var HP : Int ) {


  def Damage (reduceHP : Int): Unit ={
    this.HP -= reduceHP
  }
  def Attack(target : Monster , sort: Sort ): Unit ={
    target.Damage(sort.power)
  }

}

