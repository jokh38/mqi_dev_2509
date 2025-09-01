
-----

## Pueue: 종합 설치 및 설정 가이드

Pueue는 장시간 실행되는 작업을 관리하기 위한 강력한 커맨드 라인 도구입니다. 명령어를 큐에 추가하고, 순차적 또는 병렬적으로 실행하며, 터미널을 닫은 후에도 작업을 관리할 수 있게 해줍니다. 이 가이드는 Windows와 Ubuntu 환경에 Pueue를 설치하고, 원격으로 작업을 관리할 수 있도록 클라이언트-데몬 환경을 설정하는 상세한 방법을 제공합니다.

-----

### 1\. Pueue 설치 및 데몬 실행 (Windows)

Windows에서는 미리 컴파일된 실행 파일을 사용하여 Pueue를 설치합니다. 데몬(`pueued`)이 항상 백그라운드에서 실행되도록 Windows 작업 스케줄러를 이용해 자동 시작 설정을 진행합니다.

#### 1.1. 설치 파일 다운로드

1.  **Pueue 릴리스 페이지 접속:** 웹 브라우저에서 공식 GitHub 릴리스 페이지로 이동합니다. ([https://github.com/Nukesor/pueue/releases](https://github.com/Nukesor/pueue/releases))
2.  **Windows용 아카이브 다운로드:** 최신 릴리스에서 `x86_64-pc-windows-msvc` 이름이 포함된 압축 파일을 다운로드합니다.
3.  **파일 압축 해제:** 다운로드한 파일의 압축을 `C:\Tools\pueue`와 같이 관리하기 편한 경로에 해제합니다. 폴더 내에는 두 개의 주요 파일이 있습니다.
      * `pueue.exe`: 작업 관리를 위한 클라이언트
      * `pueued.exe`: 백그라운드에서 작업을 실행하는 데몬 (서버)

#### 1.2. 환경 변수(PATH)에 추가

명령 프롬프트나 PowerShell 어디서든 `pueue` 명령어를 바로 사용할 수 있도록 실행 파일이 있는 폴더 경로를 시스템 환경 변수 `PATH`에 추가해야 합니다.

1.  **시스템 속성 열기:** `Win + R` 키를 누르고 `sysdm.cpl`을 입력한 후 Enter 키를 누릅니다.
2.  **환경 변수 메뉴 이동:** '고급' 탭에서 '환경 변수' 버튼을 클릭합니다.
3.  **Path 변수 편집:** '시스템 변수' 목록에서 `Path` 변수를 찾아 선택하고 '편집'을 클릭합니다.
4.  **새 경로 추가:** '새로 만들기'를 클릭하여 Pueue 압축을 해제한 폴더 경로(예: `C:\Tools\pueue`)를 입력합니다.
5.  **변경 사항 저장:** 열려있는 모든 창에서 '확인'을 눌러 변경 사항을 저장합니다.

#### 1.3. 작업 스케줄러를 통한 데몬 자동 시작 설정

`pueued.exe` 데몬이 컴퓨터 시작 시 자동으로 실행되고 백그라운드에서 계속 동작하도록 Windows 작업 스케줄러에 등록합니다.

1.  **작업 스케줄러 열기:** `Win + R` 키를 누르고 `taskschd.msc`를 입력한 후 Enter 키를 누릅니다.
2.  **새 작업 만들기:** 오른쪽 '작업' 메뉴에서 '기본 작업 만들기...'를 선택합니다.
3.  **이름 및 설명 입력:** 작업 이름(예: "Pueue Daemon")을 입력하고 '다음'을 클릭합니다.
4.  **트리거 설정:** '컴퓨터 시작 시'를 선택하고 '다음'을 클릭합니다.
5.  **동작 설정:** '프로그램 시작'을 선택하고 '다음'을 클릭합니다.
6.  **프로그램 경로 지정:**
      * '프로그램/스크립트' 필드에 `pueued.exe`를 입력합니다.
      * '시작 위치(옵션)' 필드에는 `pueued.exe` 파일이 있는 폴더 경로(예: `C:\Tools\pueue`)를 입력합니다.
7.  **설정 완료:** '다음'을 누르고 '마침'을 클릭하여 작업을 생성합니다.
8.  **추가 속성 설정:**
      * 생성된 작업을 목록에서 찾아 마우스 오른쪽 버튼으로 클릭 후 '속성'을 선택합니다.
      * '일반' 탭에서 '사용자의 로그온 여부에 관계없이 실행'과 '가장 높은 수준의 권한으로 실행' 옵션을 선택합니다.
      * '조건' 탭에서 '컴퓨터의 AC 전원이 켜져 있는 경우에만 작업 시작' 옵션을 해제합니다.
      * '확인'을 눌러 설정을 저장합니다. 관리자 암호를 요구할 수 있습니다.

이제 Pueue 데몬은 시스템이 시작될 때마다 자동으로 실행됩니다. 명령 프롬프트에서 `pueue status` 명령어를 실행하여 데몬의 상태를 확인할 수 있습니다.

-----

### 2\. Pueue 설치 및 데몬 설정 (Ubuntu)

Ubuntu에서는 Rust의 패키지 매니저인 `cargo`를 사용하여 Pueue를 설치하고, `systemd`를 이용해 데몬이 안정적으로 항상 실행되도록 설정합니다.

#### 2.1. Rust를 이용한 설치

  * **`rustup` 설치**: `rustup`은 Rust 컴파일러와 관련 도구를 관리하는 공식 도구입니다. 아래 명령어로 설치합니다.

    ```bash
    curl https://sh.rustup.rs -sSf | sh
    ```

    화면의 지시에 따라 설치를 완료한 후, 터미널을 재시작하거나 `source $HOME/.cargo/env` 명령어를 실행하여 변경사항을 적용합니다.

  * **Pueue 설치**: Rust 설치가 완료되면 `cargo`를 사용하여 Pueue를 설치합니다.

    ```bash
    cargo install --locked pueue
    ```

    이 명령어는 `$HOME/.cargo/bin/` 경로에 `pueue` 실행 파일을 설치합니다.

-----

#### 2.2. `systemd`를 활용한 데몬 자동 실행 설정

시스템 부팅 시 Pueue 데몬이 자동으로 시작되고, 예기치 않게 종료될 경우 다시 시작되도록 `systemd` 서비스를 설정합니다.

  * **데몬 실행 파일 복사**:
    `cargo`로 설치된 `pueue` 바이너리 파일을 시스템 전체에서 사용할 수 있도록 `/usr/local/bin/` 경로에 `pueued`라는 이름으로 복사합니다.

    ```bash
    sudo cp $HOME/.cargo/bin/pueued /usr/local/bin/pueued
    ```

  * **`systemd` 서비스 파일 생성**:
    `/etc/systemd/system/pueue.service` 파일을 `nano`나 `vim` 같은 텍스트 편집기로 생성하고 아래 내용을 추가합니다. `your_username` 부분은 실제 사용자 이름으로 변경해야 합니다.

    ```ini
    [Unit]
    Description=Pueue Daemon
    After=network.target

    [Service]
    User=your_username
    Group=your_username
    ExecStart=/usr/local/bin/pueued --config /home/your_username/.config/pueue/pueue.yml
    Restart=always
    RestartSec=5s

    [Install]
    WantedBy=multi-user.target
    ```

  * **`systemd` 설정 적용 및 서비스 시작**:
    새로운 서비스 파일을 `systemd`가 인식하도록 설정을 리로드합니다.

    ```bash
    sudo systemctl daemon-reload
    ```

    부팅 시 `pueue` 서비스가 자동으로 시작되도록 활성화하고, 지금 바로 서비스를 시작합니다.

    ```bash
    sudo systemctl enable pueue.service
    sudo systemctl start pueue.service
    ```

  * **서비스 상태 확인**:
    `systemctl status pueue.service` 명령어로 서비스가 정상적으로 실행 중인지 확인할 수 있습니다.

    ```bash
    systemctl status pueue.service
    ```

    출력 결과에서 `active (running)` 상태를 확인하면 됩니다.

-----

### 3\. 원격 접속 문제 해결 (Windows 클라이언트 → Ubuntu 데몬)

이 섹션에서는 Windows PC에 설치된 Pueue 클라이언트를 사용하여 원격 Ubuntu PC에서 실행 중인 Pueue 데몬을 관리하는 방법을 설명합니다.

**문제 원인:**

1.  **데몬의 원격 접속 미허용:** Ubuntu 데몬은 기본적으로 로컬 통신을 위한 유닉스 소켓을 사용하므로 외부 네트워크 연결을 허용하지 않습니다.
2.  **설정 불일치:** 클라이언트의 `pueue.yml` 설정 파일에 있는 `host`와 `port` 정보가 Ubuntu 데몬의 주소와 일치하지 않습니다.
3.  **인증서 및 비밀키 누락:** 클라이언트와 데몬 간의 보안 통신에 필요한 인증 파일(`daemon.cert`, `shared_secret`)이 클라이언트 측에 없습니다.

**해결 과정:**

#### 3.1. Ubuntu 측 (데몬) 설정

1.  **`pueue.yml` 설정 파일 편집:**
    `~/.config/pueue/pueue.yml` 파일을 엽니다. 파일이 없다면 `pueued`를 한 번 실행하여 생성합니다. `shared` 섹션에서 아래와 같이 수정합니다.

      * `use_unix_socket`: `false`로 변경합니다.
      * `host`: `0.0.0.0` (모든 네트워크 인터페이스에서 접속 허용) 또는 Ubuntu PC의 내부 IP 주소로 변경합니다.
      * `port`: 원격 접속에 사용할 포트 번호(예: `6920`)를 지정합니다.

    **설정 예시 (`pueue.yml`):**

    ```yaml
    shared:
      pueue_directory: /home/your_username/.local/share/pueue
      use_unix_socket: false
      host: "0.0.0.0"
      port: "6920"
      # 이하 경로는 대부분 기본값이므로 수정할 필요가 없습니다.
      daemon_cert: /home/your_username/.local/share/pueue/certs/daemon.cert
      daemon_key: /home/your_username/.local/share/pueue/certs/daemon.key
      shared_secret_path: /home/your_username/.local/share/pueue/shared_secret

    # client 및 daemon 섹션은 필요에 따라 설정합니다.
    ```

2.  **Pueue 데몬 재시작:**
    `systemd` 서비스를 사용 중이므로, 아래 명령어로 서비스를 재시작하여 설정을 적용합니다.

    ```bash
    sudo systemctl restart pueue.service
    ```

3.  **포트 리스닝 확인:**
    데몬이 지정된 TCP 포트를 정상적으로 수신 대기하고 있는지 확인합니다.

    ```bash
    sudo netstat -tuln | grep 6920
    ```

4.  **방화벽 설정 (필요 시):**
    `ufw` 방화벽이 활성화된 경우, 외부에서 해당 포트로의 접속을 허용해야 합니다.

    ```bash
    sudo ufw allow 6920/tcp
    sudo ufw reload
    ```

#### 3.2. Windows 측 (클라이언트) 설정

1.  **인증 파일 복사:**
    Ubuntu PC에서 아래 두 파일을 Windows PC로 복사해야 합니다.

      * `/home/your_username/.local/share/pueue/certs/daemon.cert`
      * `/home/your_username/.local/share/pueue/shared_secret`

    이 파일들을 Windows의 Pueue 설정 폴더로 복사합니다. 해당 폴더의 기본 경로는 다음과 같습니다.
    `%USERPROFILE%\AppData\Local\pueue\`

    파일 전송에는 `scp`, `WinSCP` 등의 도구를 사용할 수 있습니다.

2.  **Windows에서 `pueue.yml` 설정 파일 편집:**
    Windows PC의 `%USERPROFILE%\.config\pueue\` 폴더에 있는 `pueue.yml` 파일을 엽니다. 파일이 없다면 `pueue status`를 한 번 실행하여 생성합니다. `shared` 섹션을 아래와 같이 수정합니다.

    **설정 예시 (`pueue.yml`):**

    ```yaml
    shared:
      pueue_directory: C:\Users\YourUser\AppData\Local\pueue
      use_unix_socket: false
      host: "192.168.1.100" # Ubuntu PC의 IP 주소로 변경
      port: "6920"          # Ubuntu 데몬과 동일한 포트 번호
      # 아래 경로는 복사한 파일 위치를 가리키도록 자동 설정됩니다.
      daemon_cert: C:\Users\YourUser\AppData\Local\pueue\daemon.cert
      daemon_key: C:\Users\YourUser\AppData\Local\pueue\daemon.key
      shared_secret_path: C:\Users\YourUser\AppData\Local\pueue\shared_secret

    client:
      read_local_logs: true
      show_expanded_aliases: false
    ```

#### 3.3. 연결 확인

모든 설정이 완료되면 Windows PC의 명령 프롬프트에서 `pueue status`를 실행합니다. 원격 Ubuntu 서버의 작업 큐 상태가 정상적으로 보인다면 연결에 성공한 것입니다.