namespace FacebookAPI___PageAPI.models
{
    public class ApiResponse
    {
        public bool Success { get; set; }
        public string Message { get; set; } = string.Empty;
        public object? Data { get; set; }
        public object? Error { get; set; }
    }
}
