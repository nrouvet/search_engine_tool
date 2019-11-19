import com.sun.prism.impl.Disposer.Target
import org.apache.spark.sql.catalyst.plans.logical.Sort

case class Monster(var id : Int, var name : String, var equipe : String, var armure: Int, var HP : Int, var listSort: List[Sort] ) {
//case class Monster(var id : Int, var name : String, var equipe : String, var armure: Int, var HP : Int){

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

