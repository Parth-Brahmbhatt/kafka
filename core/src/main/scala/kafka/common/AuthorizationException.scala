package kafka.common

/**
 * Exception thrown when a principal is not authorized to perform an operation.
 * @param message
 */
class AuthorizationException(message: String) extends RuntimeException(message) {
}
