package dev.kamu.cli.macros

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

case class FieldDescriptor(
  name: String,
  typ: String
  //applier: (T, String) => T
)

object Magic {
  def magic[T](): List[FieldDescriptor] = macro MagicImpl.magic[T]
}

class MagicImpl(val c: blackbox.Context) {
  import c.universe._

  final def magic[T: c.WeakTypeTag]() = {
    val typ = weakTypeOf[T]
    val fields =
      typ.members
        .filter(_.isMethod)
        .map(_.asMethod)
        .filter(_.isCaseAccessor)

    fields.map(_.returnType)

    val x = fields
      .map(f => (f.name.toString, f.returnType.toString))
      .map {
        case (name, t) =>
          q"""dev.kamu.cli.macros.FieldDescriptor(name = $name, typ = $t)"""
      }
      .toList
      .reverse

    //applier = (obj, fieldVal) => obj.copy($name = fieldVal)

    c.Expr(q"""List(..$x)""")
  }
}
