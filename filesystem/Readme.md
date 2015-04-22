# Azure Storage Client
This program provides filesystem interfaces that make it easy to access Azure Storage Client.

# Installation
- Install Golang: https://golang.org/doc/install
- Get Azure SDK package: 

```sh
go get github.com/MSOpenTech/azure-sdk-for-go
```
- Install: 

```sh
go install github.com/MSOpenTech/azure-sdk-for-go
```

# Deployment

- follow http://azure.microsoft.com/en-gb/documentation/articles/storage-create-storage-account/ to create Azure Storage. 
- Get your Azure Storage Accounts, Keys and BlobServiceBaseUrl. Like http://mystorageaccount.file.core.windows.net, the BlobServiceBaserUrl should be core.windows.net
- Use NewClient function, specify whether to use HTTPS, a specific REST API version

Example:

```
    import "github.com/taskgraph/taskgraph/filesystem"
    ...
    ...
    cli, err := filesystem.NewAzureClient(
        AzurestorageAccountName, 
        AzurestorageAccountKey, 
        "core.chinacloudapi.cn", 
        "2014-02-14", 
        true
    )
```

# License
[Apache 2.0](LICENSE-2.0.txt)