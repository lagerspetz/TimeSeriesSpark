class Message(val tag:String, val num:Long, val date: String, val sender: String, val month: String,
val day: Int, val time: String, val msg:String) extends Serializable{

    override def toString() = {
        tag + " " + num + " " + date + " " + sender + " " + month + " " + day + " " + time + " " + msg
    }
}

// vim: set ts=4 sw=4 et:
