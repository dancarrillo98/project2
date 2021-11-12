package clients

/** Defines the behavior for all client classes this app may support
 *
 */
trait Client[T] {

    def Connect() : Unit

    def Create(t: T): Unit

    def Read(): T

    def Update(t: T): Unit

    def Delete(t: T): Unit

    def Close(): Unit
}
