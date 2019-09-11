package dev.kamu.cli.utility

class DependencyGraph[A](dependsOn: A => List[A]) {
  // TODO: Resolve should report encountered loops instead of not terminating
  def resolve(goal: List[A]): Seq[A] = {
    type State = (Seq[A], Set[A])
    def add(x: A)(s: State): State = (s._1 :+ x, s._2 + x)
    def isQueued(s: State)(x: A): Boolean = s._2.contains(x)
    val emptyState = (Seq.empty[A], Set.empty[A])

    @scala.annotation.tailrec
    def queue(s: State, stack: List[A]): State = {
      if (stack.nonEmpty) {
        if (isQueued(s)(stack.head)) queue(s, stack.tail)
        else {
          val deps = dependsOn(stack.head).filterNot(isQueued(s))
          if (deps.isEmpty) queue(add(stack.head)(s), stack.tail)
          else queue(s, deps ++ stack)
        }
      } else s
    }
    queue(emptyState, goal)._1
  }

}
