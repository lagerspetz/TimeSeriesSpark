package spark.timeseries.examples

object Pt4Processor {

  // fixed file offsets 
  val headerOffset = 0
  val statusOffset = 272
  val sampleOffset = 1024

  // bitmasks 
  val coarseMask = 1
  val marker0Mask = 1
  val marker1Mask = 2
  val markerMask = marker0Mask | marker1Mask

  // missing data indicators 
  val missingRawCurrent = 0x8001
  val missingRawVoltage = 0xffff

  // Enums for file header
  object CalibrationStatus extends Enumeration {
    type CalibrationStatus = Value
    val OK, Failed = Value
  }

  object VoutSetting extends Enumeration {
    type VoutSetting = Value
    val Typical, Low, High, Custom = Value
  }

  val chanMain = 0x1000
  val chanUsb = 0x2000
  val chanAux = 0x4000
  val chanMarker = 0x8000
  val chanMask = 0xf000

  // File header structure 
  class Pt4Header(
    val headSize: Int,
    val name: String,
    val batterySize: Int,
    //val captureDate:Date,
    val serialNumber: String,
    //val calibrationStatus:CalibrationStatus,
    //val voutSetting:VoutSetting,
    //val voutValue:Float,
    val hardwareRate: Int,
    //val softwareRate:Float, // ignore 
    /*val powerField:SelectField,
      val currentField:SelectField,
      val voltageField:SelectField,*/ // ignore
    val captureSetting: String,
    val swVersion: String,
    //val runMode:Int,

    val exitCode: Int,
    val totalCount: Long,
    val statusOffset: Int,
    val statusSize: Int,
    val sampleOffset: Int,
    val sampleSize: Int,
    val initialMainVoltage: Int,
    val initialUsbVoltage: Int,
    val initialAuxVoltage: Int,
    val captureDataMask: Int,
    val sampleCount: Long,
    val missingCount: Long,
    val avgMainVoltage: Float,
    val avgMainCurrent: Float,
    val avgMainPower: Double,
    val avgUsbVoltage: Double,
    val avgUsbCurrent: Double,
    val avgUsbPower: Float,
    val avgAuxVoltage: Float,
    val avgAuxCurrent: Float,
    val avgAuxPower: Float) {}

  def csReadString(f: java.io.RandomAccessFile) = {
    var stringLength = 0
    var stringLengthParsed = false
    var step = 0
    while (!stringLengthParsed) {
      var part = f.readUnsignedByte();
      stringLengthParsed = ((part.asInstanceOf[Int] >> 7) == 0)
      var partCutter: Int = part & 127
      part = partCutter.asInstanceOf[Byte]
      var toAdd: Int = part.asInstanceOf[Int] << (step * 7)
      stringLength += toAdd
      step += 1
    }
    var chars = new Array[Char](stringLength)
    var i = 0
    while (i < stringLength) {
      chars(i) = f.readByte().asInstanceOf[Char]
      i += 1
    }
    new String(chars)
  }
  
  def readLong(reader: java.io.RandomAccessFile) = {
    val b1 = reader.readUnsignedByte()
    val b2 = reader.readUnsignedByte()
    val b3 = reader.readUnsignedByte()
    val b4 = reader.readUnsignedByte()
    val b5 = reader.readUnsignedByte()
    val b6 = reader.readUnsignedByte()
    val b7 = reader.readUnsignedByte()
    val b8 = reader.readUnsignedByte()
    b1 + (b2 << 8L) + (b3 << 16L) + (b4 << 24L) + (b5 << 32L) + (b6 << 40L) + (b7 << 48L) + (b8 << 56L)
  }

  def readHeader(reader: java.io.RandomAccessFile) = {
    // remember original position 
    val oldPos = reader.getFilePointer()

    // move to start of file 
    reader.seek(0)

    // read file header 
    val headSize = (reader.readUnsignedShort >> 8) + reader.readUnsignedShort();
    val name = csReadString(reader).trim();
    val batterySize = (reader.readUnsignedShort >> 8) + reader.readUnsignedShort();
    //println("headSize=" + headSize + " name=" + name + " batterySize=" + batterySize);
    /*val  = 
       DateTime.FromBinary(*/
    val captureDate = new java.util.Date(readLong(reader)/10000+new java.util.Date(70,0,1).getTime())
    //); 
    val serialNumber = csReadString(reader).trim();
     //println("headSize=" + headSize + " name=" + name + " batterySize=" + batterySize + " serial=" + serialNumber);
    /*val calibrationStatus = 
       (CalibrationStatus)*/ (reader.readUnsignedShort >> 8) + reader.readUnsignedShort();
    /*val voutSetting = (VoutSetting)*/ (reader.readUnsignedShort >> 8) + reader.readUnsignedShort();
    /*val voutValue =*/ reader.readFloat();
    val hardwareRate = (reader.readUnsignedShort >> 8) + reader.readUnsignedShort();
    reader.readFloat(); // ignore software rate
    // ignore selection fields
    //val powerField = (SelectField)
    (reader.readUnsignedShort >> 8) + reader.readUnsignedShort();
    //val currentField = (SelectField)
    (reader.readUnsignedShort >> 8) + reader.readUnsignedShort();
    //val voltageField = (SelectField)
    (reader.readUnsignedShort >> 8) + reader.readUnsignedShort();

    val captureSetting = csReadString(reader).trim();
    val swVersion = csReadString(reader).trim();
    //val runMode = (RunMode)
    // gui or nogui, ignore
    (reader.readUnsignedShort >> 8) + reader.readUnsignedShort();
    val exitCode = (reader.readUnsignedShort >> 8) + reader.readUnsignedShort();
    val totalCount = reader.readLong()
    val statusOffset = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val statusSize = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val sampleOffset = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val sampleSize = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val initialMainVoltage = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val initialUsbVoltage = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val initialAuxVoltage = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val cdmPos = reader.getFilePointer()
    val captureDataMask = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val sampleCount = readLong(reader) //UInt64(); // FIXME: may cause issues!! 
    val missingCount = readLong(reader) //ReadUInt64();  // FIXME: may cause issues!!

    printf("%s %s %s %s %s %s %s %s %s\n",
            headSize, name, batterySize, captureDate,
            serialNumber, captureDataMask, sampleCount, missingCount, cdmPos);

    val count = Math.max(1, sampleCount -
      missingCount)

    // convert sums to averages 
    val avgMainVoltage = reader.readFloat() / count;
    val avgMainCurrent = reader.readFloat() / count;
    val avgMainPower = reader.readFloat() / count;
    val avgUsbVoltage = reader.readFloat() / count;
    val avgUsbCurrent = reader.readFloat() / count;
    val avgUsbPower = reader.readFloat() / count;
    val avgAuxVoltage = reader.readFloat() / count;
    val avgAuxCurrent = reader.readFloat() / count;
    val avgAuxPower = reader.readFloat() / count;

    // restore original position 
    reader.seek(oldPos)

    new Pt4Header(headSize, name, batterySize, serialNumber, hardwareRate, captureSetting, swVersion, exitCode, totalCount,
      statusOffset, statusSize, sampleOffset, sampleSize,
      initialMainVoltage, initialUsbVoltage, initialAuxVoltage,
      captureDataMask, sampleCount, missingCount,
      avgMainVoltage, avgMainCurrent, avgMainPower,
      avgUsbVoltage, avgUsbCurrent, avgUsbPower,
      avgAuxVoltage, avgAuxCurrent, avgAuxPower)
  }

  class StatusPacket(
    val outputVoltageSetting: Int,
    /* 
      val sbyte temperature; 
      val PmStatus pmStatus; 
      val byte reserved;*/
    val leds: Int,
    /*val sbyte mainFineResistorOffset; 
      val ushort serialNumber;*/
    val sampleRate: Int,
    /*      val ushort dacCalLow; 
      val ushort dacCalHigh; 
      val ushort powerupCurrentLimit; 
      val ushort runtimeCurrentLimit; 
      val byte powerupTime; 
      val sbyte usbFineResistorOffset; 
      val sbyte auxFineResistorOffset;*/
    val initialUsbVoltage: Int,
    val initialAuxVoltage: Int,
    val hardwareRevision: Int,
    /*val byte temperatureLimit; */
    val usbPassthroughMode: Int /*val sbyte mainCoarseResistorOffset
      val sbyte usbCoarseResistorOffset; 
      val sbyte auxCoarseResistorOffset; 
      val sbyte factoryMainFineResistorOffset; 
      val sbyte factoryUsbFineResistorOffset; 
      val sbyte factoryAuxFineResistorOffset; 
      val sbyte factoryMainCoarseResistorOffset; 
      val sbyte factoryUsbCoarseResistorOffset; 
      val sbyte factoryAuxCoarseResistorOffset; 
      val EventCode eventCode; 

 
      val ushort eventData; 
      val byte checksum;*/ ) {}

  val revA = 1
  val revB = 2
  val revC = 3
  val revD = 4

  object UsbPassthroughMode extends Enumeration {
    type UsbPassthroughMode = Value
    val off, on, auto, trigger, sync = Value
  }

  def readStatusPacket(reader: java.io.RandomAccessFile) = {
    // remember origibal position 
    val oldPos = reader.getFilePointer()

    // move to start of status packet 
    reader.seek(statusOffset)

    // read status packet 
    val packetLength = reader.readUnsignedByte();
    val packetType = reader.readUnsignedByte();
    if (packetType != 0x10) {
      println("Buggy data, wrong packetType for status packet!")
      System.exit(1)
    }
    val firmwareVersion = reader.readUnsignedByte();
    val protocolVersion = reader.readUnsignedByte();
    if (protocolVersion < 16) {
      println("Buggy data, protocolVersion < 16!")
      System.exit(1)
    }
    /*val fineObs.mainCurrent = */ reader.readByte()  + (reader.readByte() << 8);
    /*val fineObs.usbCurrent =*/ reader.readByte()  + (reader.readByte() << 8);
    /*val fineObs.auxCurrent =*/ reader.readByte()  + (reader.readByte() << 8);
    /*val fineObs.voltage =*/ reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    /*val coarseObs.mainCurrent =*/ reader.readByte()  + (reader.readByte() << 8);
    /*val coarseObs.usbCurrent =*/ reader.readByte()  + (reader.readByte() << 8);
    /*val coarseObs.auxCurrent =*/ reader.readByte()  + (reader.readByte() << 8);
    /*val coarseObs.voltage =*/ reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val outputVoltageSetting = reader.readUnsignedByte();
    val temperature = reader.readByte();
    val pmStatus = /*(PmStatus)*/ reader.readUnsignedByte();
    val reserved = reader.readUnsignedByte();
    val leds = reader.readUnsignedByte();
    val mainFineResistorOffset = reader.readByte();
    val serialNumber = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val sampleRate = reader.readUnsignedByte();
    val dacCalLow = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val dacCalHigh = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val powerupCurrentLimit = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val runtimeCurrentLimit = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val powerupTime = reader.readUnsignedByte();
    val usbFineResistorOffset = reader.readByte();
    val auxFineResistorOffset = reader.readByte();
    val initialUsbVoltage = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);

    val initialAuxVoltage = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val hardwareRevision = /*(HardwareRev)*/ reader.readUnsignedByte();
    val temperatureLimit = reader.readUnsignedByte();
    val usbPassthroughMode = reader.readUnsignedByte();
    val mainCoarseResistorOffset = reader.readByte();
    val usbCoarseResistorOffset = reader.readByte();
    val auxCoarseResistorOffset = reader.readByte();
    val factoryMainFineResistorOffset = reader.readByte();
    val factoryUsbFineResistorOffset = reader.readByte();
    val factoryAuxFineResistorOffset = reader.readByte();
    val factoryMainCoarseResistorOffset =
      reader.readByte();
    val factoryUsbCoarseResistorOffset =
      reader.readByte();
    val factoryAuxCoarseResistorOffset =
      reader.readByte();
    val eventCode = reader.readUnsignedByte();
    val eventData = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);
    val checksum = reader.readUnsignedByte();

    // restore original position 
    reader.seek(oldPos)

    new StatusPacket(outputVoltageSetting, leds, sampleRate, initialUsbVoltage, initialAuxVoltage, hardwareRevision, usbPassthroughMode)
  }

  def bytesPerSample(captureDataMask: Int) = {
    var result = 2L
    //long result = sizeof(ushort); // voltage always present 
    if ((captureDataMask & chanMain) != 0)
      result += 2

    if ((captureDataMask & chanUsb) != 0)
      result += 2

    if ((captureDataMask & chanAux) != 0)
      result += 2

    result
  }

  def SamplePosition(sampleIndex: Long,
    captureDataMask: Int) =
    {
      sampleOffset + bytesPerSample(captureDataMask) * sampleIndex
    }

  def SamplePosition(secondsOrig: Double,
    captureDataMask: Int, statusPacket: StatusPacket) = {
    val seconds = Math.max(0, secondsOrig)

    val bytesPerSampleV = bytesPerSample(captureDataMask)
    val freq = 1000 * statusPacket.sampleRate

    var result = (seconds * freq * bytesPerSampleV) toLong
    val err = result % bytesPerSampleV
    if (err > 0) // must fall on boundary 
      result += (bytesPerSampleV - err)
    result += sampleOffset
    result
  }

  def sampleCount(reader: java.io.RandomAccessFile,
    captureDataMask: Int) = {
    (reader.length() - sampleOffset) / bytesPerSample(captureDataMask)
  }

  class Sample(
    //val sampleIndex:Long,  // 0...N-1 
    val timeStamp: Double, // fractional seconds
    val mainPresent: Boolean, // whether Main was recorded 
    val mainCurrent: Double, // current in milliamps 
    val mainVoltage: Double, // volts 
    val usbPresent: Boolean, // whether Usb was recorded 
    val usbCurrent: Double, // current in milliamps 
    val usbVoltage: Double, // volts 

    val auxPresent: Boolean, // whether Aux was recorded 
    val auxCurrent: Double, // current in milliamps 
    val auxVoltage: Double, // volts; 

    val markerPresent: Boolean, // whether markers/voltages  
    //      were recorded 
    val marker0: Boolean, // Marker 0 
    val marker1: Boolean, // Marker 1 
    val missing: Boolean // true if this sample was missing 
    ) {}

  def getSample(sampleIndex: Long,
    captureDataMask: Int,
    statusPacket: StatusPacket,
    reader: java.io.RandomAccessFile): Sample = {
    // remember the index and time 
    val timeStamp = sampleIndex / (1000.0 * statusPacket.sampleRate);

    // intial settings for all flags 
    val mainPresent =
      (captureDataMask & chanMain) != 0
    val usbPresent =
      (captureDataMask & chanUsb) != 0
    val auxPresent =
      (captureDataMask & chanAux) != 0
    val markerPresent = true;
    var missing = false;

    // abort if no data was selected 
    val bytesPerSampleV = bytesPerSample(captureDataMask);
    if (bytesPerSampleV != 0) {

      // remember original position 
      val oldPos = reader.getFilePointer()

      // position the file to the start of the desired sample 
      val newPos = SamplePosition(sampleIndex, captureDataMask)
      if (oldPos != newPos)
        reader.seek(newPos)
      
      //println("Position " + newPos)
      // get default voltages (V) for the three channels 
      var mainVoltage =
        2.0 + statusPacket.outputVoltageSetting * 0.01;

      var usbVoltage =
        statusPacket.initialUsbVoltage * 125.0 / 1e6f;
      if (statusPacket.hardwareRevision < revB)
        usbVoltage /= 2;

      var auxVoltage =
        statusPacket.initialAuxVoltage * 125.0 / 1e6f;
      if (statusPacket.hardwareRevision < revC)
        auxVoltage /= 2;

      var mainCurrent = 0.0
      var auxCurrent = 0.0
      var usbCurrent = 0.0
      var marker0 = false
      var marker1 = false
      var raw: Int = 0
      // Main current (mA) 
      if (mainPresent) {
        raw = reader.readByte()  + (reader.readByte() << 8);
        //println("Raw current: " + raw)

        missing = missing ||
          raw == missingRawCurrent;
        if (!missing) {
          val coarse = (raw & coarseMask) != 0;
          raw &= ~coarseMask;
          mainCurrent = raw / 1000f; // uA -> mA 
          if (coarse)
            mainCurrent *= 250;
        }
      }

      // Aux1 current (mA) 
      if (usbPresent) {
        raw = reader.readByte()  + (reader.readByte() << 8);

        missing = missing ||
          raw == missingRawCurrent;
        if (!missing) {
          val coarse = (raw & coarseMask) != 0;
          raw &= ~coarseMask;

          usbCurrent = raw / 1000f; // uA -> mA 
          if (coarse)
            usbCurrent *= 250;
        }
      }

      // Aux2 current (mA) 
      if (auxPresent) {
        raw = reader.readByte()  + (reader.readByte() << 8);
        missing = missing ||
          raw == missingRawCurrent;
        if (!missing) {
          var coarse = (raw & coarseMask) != 0;
          raw &= ~coarseMask;
          auxCurrent = raw / 1000f; // uA -> mA 
          if (coarse)
            auxCurrent *= 250;
        }
      }

      // Markers and Voltage (V) 
      {
        var uraw = reader.readUnsignedByte()  + (reader.readUnsignedByte() << 8);

        missing = missing ||
          uraw == missingRawVoltage;
        if (!missing) {
          // strip out marker bits 
          marker0 = (uraw & marker0Mask) != 0;
          marker1 = (uraw & marker1Mask) != 0;
          uraw &= ~markerMask /*unchecked((ushort)~markerMask) */

          // calculate voltage 
          var voltage = uraw * 125.0 / 1e6f;

          // assign the high-res voltage, as appropriate 
          if ((statusPacket.leds & 0x08) != 0) {
            auxVoltage = voltage;
            if (statusPacket.hardwareRevision < revC) {
              auxVoltage /= 2;
            }
          } else {
            mainVoltage = voltage;
            if (statusPacket.hardwareRevision < revB) {
              mainVoltage /= 2;
            }
          }
        }
      }

      // restore original position, if we moved it earlier 
      if (oldPos != newPos)
        reader.seek(oldPos)
      return new Sample(timeStamp, mainPresent, mainCurrent, mainVoltage,
        usbPresent, usbCurrent, usbVoltage,
        auxPresent, auxCurrent, auxVoltage,
        markerPresent, marker0, marker1, missing)
    }
    return null
  }

  def main(args: Array[String]) {
    val fileName = args(0)
    val f = new java.io.RandomAccessFile(fileName, "r")

    val header = readHeader(f)
    val statusPacket = readStatusPacket(f)
    val scount = sampleCount(f, header.captureDataMask)
    f.seek(sampleOffset)

//    println("num, time, current, voltage");
    println("time, current");
    var sampleIndex = 0L
    while (sampleIndex < scount) {
      // read the next sample
      val sample = getSample(sampleIndex, header.captureDataMask, statusPacket, f);
      // process the sample 
//      printf("%d, %.4f, %.2f, %.4f\n", sampleIndex, sample.timeStamp, sample.mainCurrent, sample.mainVoltage)
      printf("%.4f, %.2f\n", sample.timeStamp, sample.mainCurrent)
      sampleIndex += 1
    }
    f.close()
  }
}

/**
  val fileName = "iphone4s-appuser-powermon-partial.pt4"
  val f = new java.io.RandomAccessFile(fileName, "r")
  spark.timeseries.examples.Pt4Processor.readHeader(f)

  spark.timeseries.examples.Pt4Processor.main(Array(fileName))
  
It works!!!! However, some signedness issues:
*/
