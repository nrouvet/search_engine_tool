case class Sort (var name: String, var typeSort: Boolean , var listPower : List[Int], distance: Int, var low : Int, var high : Int) {
  def this(){
    this("sort",false,List.empty[Int],0, 0, 1)
  }

}
