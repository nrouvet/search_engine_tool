import org.apache.spark.util.random

import scala.util.Random

case class Monster(var name : String, var equipe : String, var armure: Int, var HP : Int ) {


  def proba(target : Monster, start : Int, end : Int, sort: Sort): Unit ={
    var rand = start + new Random().nextInt((end-start)+1)
    if(rand + sort.power > target.armure){
      rand = 3 + new Random().nextInt((6-3)+1)
      target.Damage(rand+target.armure)
    }
  }

  def Damage (reduceHP : Int): Unit ={
    this.HP -= reduceHP
  }
  def Attack(target : Monster , sort: Sort ): Unit ={
    target.Damage(sort.power)
  }

}
