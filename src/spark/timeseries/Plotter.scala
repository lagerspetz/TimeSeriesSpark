package spark.timeseries

/*******************************************************************************
 * Copyright (c) 2009 David Orme and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     David Orme - initial API and implementation
 *******************************************************************************/

import com.coconut_palm_software.xscalawt.XScalaWT._
import com.coconut_palm_software.xscalawt.XScalaWT.Assignments._

import org.eclipse.swt.SWT
import org.eclipse.swt.widgets._
import org.eclipse.swt.layout._
import org.eclipse.swt.events._

object Plotter {
  def canvas(setups: (Canvas => Any)*) = { (parent: Composite) =>
    val canvas = new Canvas(parent, SWT.NONE)
    canvas.setLayout(new GridLayout())
    setups.foreach(setup => setup(canvas))
    canvas
  }
  
  def main(args : Array[String]) : Unit = {
    var username : Text = null

    val window = shell("Plotter",
      composite(
        layoutData = new GridData(SWT.FILL, SWT.FILL, true, true),
        layout = new GridLayout(1, false),
        canvas(
          layoutData = new GridData(SWT.FILL, SWT.FILL, true, true),
          layout = new GridLayout(1, false))),
      // Set a default shell size
      { window =>
        window.pack
        val size = window.getSize()
        window.setSize(800, 600)
      })
    
    window.getChildren()(0).asInstanceOf[Composite].getChildren()(0).addListener (SWT.Paint, new Listener () {
      def handleEvent (e: Event ) {
        val gc = e.gc
        val ca = window.getChildren()(0).asInstanceOf[Composite].getChildren()(0).asInstanceOf[Canvas].getClientArea
        val (x, y, w, h) = (ca.x, ca.y, ca.width, ca.height)
        gc.setBackground(display.getSystemColor(SWT.COLOR_BLACK))
        gc.setForeground(display.getSystemColor(SWT.COLOR_WHITE))
        gc.fillRectangle(x, y, w, h)
        gc.drawLine(x+10, y+10, w-10, h-10)
        gc.drawLine(x+10, h-10, w-10, y+10)
        
        gc.drawLine(x, y, w-1, y)
        gc.drawLine(x, y, x, h-1)
        
        gc.drawLine(w-1, y, w-1, h-1)
        gc.drawLine(x, h-1, w-1, h-1)
        
        gc.drawLine(w-1, y, w-1, h-1)
        gc.drawLine(x, h-1, w-1, h-1)
        
        gc.dispose()
      }
    })
    /*
    window.getChildren()(0).addListener (SWT.Resize, new Listener () {
    def handleEvent (e:Event) {
        //val toolBarOffset = _toolBar.getSize().y;
        val clientArea = window.getClientArea

        val canvasSize = window.getChildren()(0).getSize()

        val canvasX = Math.max(0, (clientArea.width / 2) - (canvasSize.x / 2))
        val canvasY = Math.max(0, ((clientArea.height) / 2) - (canvasSize.y / 2))

        window.getChildren()(0).setLocation(canvasX, canvasY)
      }
    });*/
    
    runEventLoop(window)
  }
}
