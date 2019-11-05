import scala.swing._
import main._

class UI extends MainFrame {

  def restrictHeight(s: Component) {
    s.maximumSize = new Dimension(Short.MaxValue, s.preferredSize.height)
  }

  title = "DonjonDragon"
  preferredSize = new Dimension(320, 240)
  val sortName = new TextField(columns = 50)
  println("Write a sort name: " + sortName.text)

  restrictHeight(sortName)
  contents = new BoxPanel(Orientation.Vertical) {
    contents += new BoxPanel(Orientation.Horizontal) {
      contents += new Label("sort Name")
      contents += Swing.HStrut(5)
      contents += sortName
    }

  }
}


object graphic extends App {
  override def main(args: Array[String]) {
    val ui = new UI
    ui.visible = true
  }
}