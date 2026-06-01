namespace RetryService.Models
{
    public class FailedEvent
    {
        public string? FailedId { get; set; }
        public string? CommandId { get; set; }
        public string? SourceTopic { get; set; }
        public string? ErrorType { get; set; }
        public string? ErrorMessage { get; set; }
        public DateTime FailedAt { get; set; }
        public int RetryCount { get; set; }
        public string? Status { get; set; }
        public string? RawEvent { get; set; }
    }
}
