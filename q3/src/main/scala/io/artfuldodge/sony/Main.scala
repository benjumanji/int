package io.artfuldodge.sony

import java.lang.{
  Long => JLong,
  StringBuilder
}
import java.util.{
  Comparator,
  List => JList
}

import scala.util.Try
import scala.collection.JavaConversions._

import com.google.cloud.dataflow.sdk.Pipeline
import com.google.cloud.dataflow.sdk.io.TextIO
import com.google.cloud.dataflow.sdk.options.{
  DataflowPipelineOptions,
  PipelineOptionsFactory
}
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
import com.google.cloud.dataflow.sdk.transforms.{
  ApproximateUnique,
  Count,
  DoFn,
  ParDo,
  PTransform,
  Top
}
import com.google.cloud.dataflow.sdk.values.{KV, PCollection, PDone}
import org.joda.time.{DateTime, Instant}

case class ArtistTrack(artist: String, track: String) extends java.io.Serializable

case class SinglePlay(
  userId: String,
  timestamp: Instant,
  artistId: String,
  artistName: String,
  trackId: String,
  trackName: String
) extends java.io.Serializable {

  def canonicalTrack: String = {
    if (trackId.isEmpty) trackName else trackId
  }

  def artistTrack = new ArtistTrack(artistName, trackName)
}

object SinglePlay {
  def parse(input: String): Option[SinglePlay] = {
    val strings = input.split('\t')
    Try(new DateTime(strings(1))).map { timestamp =>
      SinglePlay(
        strings(0),
        timestamp.toInstant,
        strings(2),
        strings(3),
        strings(4),
        strings(5)
      )
    }.toOption
  }
}

class ToSinglePlay extends PTransform[PCollection[String], PCollection[SinglePlay]] {

  val dfn = new DoFn[String, SinglePlay] {
    def processElement(c: DoFn[String, SinglePlay]#ProcessContext) {
      SinglePlay.parse(c.element).foreach(x => c.outputWithTimestamp(x, x.timestamp))
    }
  }

  override def apply(input: PCollection[String]): PCollection[SinglePlay] = {
    input.apply(ParDo.of(dfn))
  }

}

object ToSinglePlay {
  def apply(): ToSinglePlay = new ToSinglePlay
}

// Get the distinct count of tracks per user and output it
class PartA extends PTransform[PCollection[SinglePlay], PDone] {

  // convert to pairs of user id / track
  val kvs = new DoFn[SinglePlay, KV[String, String]] {
    def processElement(c: DoFn[SinglePlay, KV[String, String]]#ProcessContext) {
      c.output(KV.of(c.element.userId, c.element.canonicalTrack))
    }
  }

  // convert back to tab delimeters for output
  val lines = new DoFn[KV[String, java.lang.Long], String] {
    def processElement(c: DoFn[KV[String, java.lang.Long], String]#ProcessContext) {
      val kv = c.element
      c.output(s"${kv.getKey}\t${kv.getValue}")
    }
  }

  override def apply(input: PCollection[SinglePlay]): PDone = {
    input
      .apply(ParDo.of(kvs))                  // get pairs
      .apply(ApproximateUnique.perKey(0.01)) // max estimation error = 1%
      .apply(ParDo.of(lines))                // tab delimit
      .apply(TextIO.Write.to("gs://some/file"))
  }
}

class PartB extends PTransform[PCollection[SinglePlay], PDone] {

  // save on some typing, and make the sigs a bit tidier.
  type GrpCnt = KV[ArtistTrack, java.lang.Long]

  val dfn = new DoFn[SinglePlay, ArtistTrack] {
    def processElement(c: DoFn[SinglePlay, ArtistTrack]#ProcessContext) {
      c.output(c.element.artistTrack)
    }
  }

  val cntCompare = new Comparator[GrpCnt] with java.io.Serializable {
    def compare(x: GrpCnt, y: GrpCnt): Int = {
      // reverse the sign to get the right ordering
      val counts = (-1) * x.getValue.compareTo(y.getValue)

      if (counts != 0) counts else x.getKey.track.compareTo(y.getKey.track)
    }
  }

  val lines = new DoFn[JList[KV[ArtistTrack, JLong]], String] {
    def processElement(c: DoFn[JList[KV[ArtistTrack, JLong]], String]#ProcessContext) {
      val builder = new StringBuilder
      c.element.foreach { kv =>
        val count = kv.getValue
        kv.getKey match {
          case ArtistTrack(artist, track) => s"${artist}\t${track}\t${count}"
        }
      }

      c.output(builder.toString)
    }
  }

  override def apply(input: PCollection[SinglePlay]): PDone = {
    input
      .apply(ParDo.of(dfn))
      .apply(Count.perElement[ArtistTrack])
      .apply(Top.of(100, cntCompare))
      .apply(ParDo.of(lines))
    PDone.in(input.getPipeline)
  }

}



class Main extends App {
  val options = PipelineOptionsFactory.create().as(classOf[DataflowPipelineOptions])
  options.setRunner(classOf[DataflowPipelineRunner])

  val pipline = Pipeline.create(options)
  val text = TextIO.Read.from("gs://some/file")
  val singlePlays = pipline
    .apply(text)
    .apply(ToSinglePlay())

  singlePlays.apply(new PartA)
  singlePlays.apply(new PartB)

}


