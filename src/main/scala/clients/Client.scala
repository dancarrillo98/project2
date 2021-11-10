package clients

/** This class is an "interface" which defines the behavior for all client
 * classes this application may support
 *
 */
trait Client[T] {

    def Create(t: T): Unit

    def Read(): T

    def Update(t: T): Unit

    def Delete(t: T): Unit
}
