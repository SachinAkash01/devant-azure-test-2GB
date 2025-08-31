import ballerina/file;
import ballerina/io;
import ballerina/os;
import ballerinax/azure_storage_service.files as azure_files;

configurable string SAS = ?;
configurable string accountName = ?;
int SIZE_MB = 2048;

azure_files:ConnectionConfig fileServiceConfig = {
    accessKeyOrSAS: SAS,
    accountName: accountName,
    authorizationMethod: "SAS"
};

azure_files:FileClient fileClient = check new (fileServiceConfig);

public function main() returns error? {
    string|() tmp = os:getEnv("TMPDIR");
    string tmpDir = (tmp is string && tmp != "") ? tmp : "/tmp";
    string localFilePath = string `${tmpDir}/file-${SIZE_MB}mb.txt`;

    io:println(string `Generating ${SIZE_MB} MB test file at: ${localFilePath}`);
    check generateFile(localFilePath, SIZE_MB * 1024 * 1024);
    io:println("File generation successful!");

    file:MetaData fi = check file:getMetaData(localFilePath);
    int fileSize = fi.size;

    string remoteName = string `file-${SIZE_MB}mb.txt`;
    check fileClient->createFile(fileShareName = "testf1", newFileName = remoteName, fileSizeInByte = fileSize, azureDirectoryPath = "test-smb");
    io:println("File created successfully!");
    check fileClient->putRange(fileShareName = "testf1", localFilePath = localFilePath, azureFileName = remoteName, azureDirectoryPath = "test-smb");
    io:println("Upload complete.");
}

function generateFile(string filePath, int size) returns error? {
    string line = "This is a perf test line for Azure Files.\n";
    int targetChunkBytes = 4 * 1024 * 1024;

    string chunkStr = line;
    while (chunkStr.toBytes().length() < targetChunkBytes) {
        chunkStr += line;
    }
    byte[] chunk = chunkStr.toBytes();

    io:WritableByteChannel ch = check io:openWritableFile(filePath);

    int total = 0;
    while (total + chunk.length() <= size) {
        _ = check ch.write(chunk, 0);
        total += chunk.length();
    }

    int remaining = size - total;
    if (remaining > 0) {
        byte[] tail = chunk.slice(0, remaining);
        _ = check ch.write(tail, 0);
    }
    check ch.close();
}
