import com.sun.prism.impl.Disposer.Target
import org.apache.spark.sql.catalyst.plans.logical.Sort

import scala.util.Random

case class Monster(var id : Int, var name : String, var equipe : String, var armure: Int, var HP : Int, var listSort: List[Sort], var counterAtt : Int, var maxAtt : Int) {
//case class Monster(var id : Int, var name : String, var equipe : String, var armure: Int, var HP : Int){

  def Damage (reduceHP : Int): Unit ={
    this.HP -= reduceHP
  }
  def Heal (increaseHP : Int): Unit ={
    this.HP += increaseHP
  }


  def Attack(target : Monster , sort: Sort): Unit ={
    val r = new Random()
    val rand = sort.low + r.nextInt(sort.high - sort.low)
    if(sort.typeSort == false){
      target.Damage(sort.listPower(counterAtt) + rand)
    }
    else
      target.Heal(sort.listPower(counterAtt) + rand)
  }


  def ChoiceSort(target: Monster , monster: Monster , distance : Int): Unit =
  {

    for( i <- 0 to monster.listSort.length){
      if(monster.listSort(i).distance >= distance)
        {
          var choiceSort : Sort = listSort(i)
          return choiceSort
        }
      else
         {
           null
         }
    }
  }

  def Regeneration(monster :Monster, regeneration: Int ): Unit =
  {
    this.HP +=  regeneration
  }





}

